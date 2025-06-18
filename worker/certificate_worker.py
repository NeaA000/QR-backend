# worker/certificate_worker.py - 인덱스 없이 작동하는 버전

import os
import io
import time
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
import json

# ===================================================================
# 로깅 설정
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('CertificateWorker')

# ===================================================================
# 환경변수 검증
# ===================================================================
required_env_vars = ['type', 'project_id', 'private_key', 'client_email']
for var in required_env_vars:
    if not os.environ.get(var):
        logger.error(f"필수 환경변수 누락: {var}")
        sys.exit(1)

# 설정값
POLL_INTERVAL_SECONDS = max(30, int(os.getenv('POLL_INTERVAL_SECONDS', '45')))
BATCH_SIZE = min(20, int(os.getenv('BATCH_SIZE', '10')))  # 인덱스 없이는 작게
MASTER_FILENAME = "master_certificates.xlsx"
MAX_RETRY_COUNT = 5
HEALTH_CHECK_INTERVAL = 300

logger.info(f"🚀 설정 완료 - 폴링:{POLL_INTERVAL_SECONDS}초, 배치:{BATCH_SIZE}개")

# ===================================================================
# Firebase 초기화
# ===================================================================
def initialize_firebase():
    """Firebase 초기화"""
    try:
        if firebase_admin._apps:
            return
        
        firebase_creds = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ.get("private_key_id", ""),
            "private_key": os.environ["private_key"].replace('\\n', '\n'),
            "client_email": os.environ["client_email"],
            "client_id": os.environ.get("client_id", ""),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.environ.get("client_x509_cert_url", "")
        }
        
        cred = credentials.Certificate(firebase_creds)
        app = firebase_admin.initialize_app(cred, {
            'storageBucket': f"{os.environ['project_id']}.appspot.com"
        })
        
        logger.info("✅ Firebase 초기화 완료")
        
    except Exception as e:
        logger.error(f"❌ Firebase 초기화 실패: {e}")
        sys.exit(1)

# Firebase 초기화
initialize_firebase()
db = firestore.client()
bucket = storage.bucket()

# ===================================================================
# 안전한 종료 관리
# ===================================================================
shutdown_flag = False
current_operations = set()
operations_lock = threading.Lock()

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 수신 - 안전한 종료 시작")
    shutdown_flag = True
    
    with operations_lock:
        if current_operations:
            logger.info(f"📋 {len(current_operations)}개 작업 완료 대기")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def log_operation_start(operation_id):
    with operations_lock:
        current_operations.add(operation_id)

def log_operation_end(operation_id):
    with operations_lock:
        current_operations.discard(operation_id)

# ===================================================================
# 인덱스 없이 작동하는 수료증 조회
# ===================================================================
def get_pending_certificates_no_index(limit=20):
    """
    인덱스 없이 작동하는 수료증 조회 - 개별 사용자 검색
    """
    operation_id = f"get_pending_no_index_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        logger.info("🔍 인덱스 없이 수료증 조회 시작 (개별 사용자 검색)")
        
        # 1단계: 활성 사용자들 조회
        try:
            # 최근 30일 내 활동한 사용자들 조회
            recent_time = datetime.now(timezone.utc) - timedelta(days=30)
            
            # users 컬렉션에서 최근 로그인 사용자 조회
            users_query = db.collection('users') \
                           .where('lastLogin', '>=', recent_time) \
                           .limit(100)  # 최대 100명
            
            active_users = list(users_query.stream())
            logger.info(f"📊 최근 30일 내 활성 사용자: {len(active_users)}명")
            
        except Exception as e:
            logger.warning(f"⚠️ 활성 사용자 조회 실패, 전체 사용자 조회: {e}")
            # 폴백: 전체 사용자에서 랜덤 샘플링
            all_users_query = db.collection('users').limit(50)
            active_users = list(all_users_query.stream())
            logger.info(f"📊 전체 사용자 샘플: {len(active_users)}명")
        
        if not active_users:
            logger.info("😴 활성 사용자가 없습니다")
            return []
        
        # 2단계: 각 사용자별로 수료증 검색
        results = []
        checked_users = 0
        found_certs = 0
        
        for user_doc in active_users:
            if shutdown_flag or len(results) >= limit:
                break
                
            user_uid = user_doc.id
            checked_users += 1
            
            try:
                # 해당 사용자의 수료증 서브컬렉션 조회
                user_certs_ref = db.collection('users').document(user_uid) \
                                  .collection('completedCertificates')
                
                # 단순 쿼리 (인덱스 불필요)
                # excelUpdated=false 조건만 먼저 적용
                cert_docs = list(user_certs_ref.where('excelUpdated', '==', False).limit(5).stream())
                
                for cert_doc in cert_docs:
                    if len(results) >= limit:
                        break
                    
                    try:
                        cert_data = cert_doc.to_dict()
                        
                        # 메모리에서 추가 필터링
                        # 1. PDF URL 체크
                        pdf_url = cert_data.get('pdfUrl', '').strip()
                        if not pdf_url:
                            continue
                        
                        # 2. sentToAdmin 체크
                        sent_to_admin = cert_data.get('sentToAdmin', False)
                        if not sent_to_admin:
                            continue
                        
                        # 3. 재시도 횟수 체크
                        retry_count = cert_data.get('retryCount', 0)
                        if retry_count >= MAX_RETRY_COUNT:
                            # 재시도 리셋 옵션
                            reset_enabled = os.getenv('RESET_HIGH_RETRY_COUNT', 'false').lower() == 'true'
                            if reset_enabled and retry_count <= 10:
                                logger.info(f"🔄 재시도 카운터 리셋: {user_uid[:8]}.../{cert_doc.id[:8]}...")
                                try:
                                    cert_doc.reference.update({
                                        'retryCount': 0,
                                        'resetAt': firestore.SERVER_TIMESTAMP,
                                        'resetReason': 'no_index_worker_reset'
                                    })
                                except Exception:
                                    continue
                            else:
                                continue
                        
                        # 모든 조건을 만족하는 수료증 발견
                        results.append((user_uid, cert_doc.id, cert_data))
                        found_certs += 1
                        
                        if len(results) <= 3:  # 처음 3개만 상세 로그
                            lecture_title = cert_data.get('lectureTitle', '제목없음')
                            logger.info(f"✅ 처리 대상 발견: {user_uid[:8]}.../{cert_doc.id[:8]}... - {lecture_title[:30]}...")
                        
                    except Exception as e:
                        logger.debug(f"수료증 문서 처리 오류: {e}")
                        continue
                        
            except Exception as e:
                logger.debug(f"사용자 {user_uid[:8]}... 수료증 조회 실패: {e}")
                continue
            
            # 진행 상황 로깅
            if checked_users % 10 == 0:
                logger.info(f"📊 진행 상황: {checked_users}/{len(active_users)}명 확인, {found_certs}개 수료증 발견")
        
        logger.info(f"🎯 최종 결과:")
        logger.info(f"  👥 확인한 사용자: {checked_users}명")
        logger.info(f"  📋 발견한 수료증: {found_certs}개")
        logger.info(f"  ✅ 처리 가능: {len(results)}개")
        
        if len(results) == 0:
            logger.info("💡 처리 가능한 수료증이 없습니다")
            logger.info("   - 사용자들이 '관리자 전송' 버튼을 클릭했는지 확인")
            logger.info("   - sentToAdmin=true, pdfUrl 존재, excelUpdated=false 조건 확인")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 인덱스 없는 수료증 조회 실패: {e}")
        return []
    finally:
        log_operation_end(operation_id)

def get_user_info(user_uid):
    """사용자 정보 조회 (캐싱)"""
    if not hasattr(get_user_info, 'cache'):
        get_user_info.cache = {}
    
    if user_uid in get_user_info.cache:
        return get_user_info.cache[user_uid]
    
    try:
        user_doc = db.collection('users').document(user_uid).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            user_info = {
                'name': user_data.get('name', ''),
                'phone': user_data.get('phone', ''),
                'email': user_data.get('email', '')
            }
            
            if len(get_user_info.cache) < 1000:
                get_user_info.cache[user_uid] = user_info
            
            return user_info
        else:
            empty_info = {'name': '', 'phone': '', 'email': ''}
            get_user_info.cache[user_uid] = empty_info
            return empty_info
            
    except Exception as e:
        logger.debug(f"사용자 정보 조회 실패: {e}")
        return {'name': '', 'phone': '', 'email': ''}

# ===================================================================
# Excel 처리 함수들
# ===================================================================
def load_master_excel():
    """마스터 엑셀 로드"""
    operation_id = f"load_excel_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        try:
            master_blob = bucket.blob(MASTER_FILENAME)
            existing_bytes = master_blob.download_as_bytes()
            
            excel_buffer = io.BytesIO(existing_bytes)
            df = pd.read_excel(excel_buffer, engine='openpyxl')
            
            expected_columns = [
                '업데이트 날짜', '사용자 UID', '전화번호', '이메일', 
                '사용자 이름', '강의 제목', '발급 일시', 'PDF URL'
            ]
            
            if not all(col in df.columns for col in expected_columns):
                logger.warning("⚠️ 엑셀 컬럼 구조 이상, 새로 생성")
                return create_empty_dataframe()
            
            if len(df) > 15000:
                logger.warning("⚠️ 데이터 크기 제한으로 최근 10,000행만 유지")
                df = df.tail(10000).reset_index(drop=True)
            
            logger.info(f"✅ 엑셀 로드 완료 (행 수: {len(df)})")
            return df
            
        except Exception:
            logger.info("⚠️ Firebase Storage 로드 실패, 새 파일 생성")
            return create_empty_dataframe()
            
    except Exception as e:
        logger.error(f"❌ 엑셀 로드 실패: {e}")
        return create_empty_dataframe()
    finally:
        log_operation_end(operation_id)

def create_empty_dataframe():
    """빈 DataFrame 생성"""
    return pd.DataFrame(columns=[
        '업데이트 날짜',
        '사용자 UID',
        '전화번호',
        '이메일',
        '사용자 이름',
        '강의 제목',
        '발급 일시',
        'PDF URL'
    ])

def save_master_excel(df):
    """마스터 엑셀 저장"""
    operation_id = f"save_excel_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        if len(df) > 20000:
            logger.warning("⚠️ 저장 크기 제한으로 최근 15,000행만 저장")
            df = df.tail(15000).reset_index(drop=True)
        
        out_buffer = io.BytesIO()
        with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Certificates')
        out_buffer.seek(0)
        
        backup_path = Path(f'/tmp/backup_{int(time.time())}.xlsx')
        try:
            with open(backup_path, 'wb') as f:
                f.write(out_buffer.getvalue())
            logger.info("💾 로컬 백업 완료")
        except Exception:
            logger.debug("로컬 백업 실패")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                out_buffer.seek(0)
                master_blob = bucket.blob(MASTER_FILENAME)
                
                master_blob.upload_from_file(
                    out_buffer,
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                logger.info(f"✅ 엑셀 저장 완료 (총 {len(df)}행)")
                
                try:
                    backup_path.unlink()
                except Exception:
                    pass
                
                return True
                
            except Exception as e:
                logger.warning(f"⚠️ 엑셀 저장 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return False
        
        return False
        
    except Exception as e:
        logger.error(f"❌ 엑셀 저장 중 예외: {e}")
        return False
    finally:
        log_operation_end(operation_id)

# ===================================================================
# 수료증 처리 함수들
# ===================================================================
def process_certificate(user_uid, cert_id, cert_data, df):
    """단일 수료증 처리"""
    try:
        user_info = get_user_info(user_uid)
        
        lecture_title = cert_data.get('lectureTitle', cert_id)
        pdf_url = cert_data.get('pdfUrl', '')
        
        if not pdf_url.strip():
            raise ValueError("PDF URL이 비어있습니다")
        
        issued_at = cert_data.get('issuedAt')
        if hasattr(issued_at, 'to_datetime'):
            issued_str = issued_at.to_datetime().strftime('%Y-%m-%d %H:%M:%S')
        else:
            issued_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        # 중복 확인
        existing_mask = (
            (df['사용자 UID'] == user_uid) & 
            (df['강의 제목'] == lecture_title) & 
            (df['PDF URL'] == pdf_url)
        )
        
        if existing_mask.any():
            logger.info(f"⚠️ 중복 수료증 스킵: {user_uid[:8]}.../{lecture_title[:20]}...")
            return True, df
        
        updated_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        new_row = pd.DataFrame([{
            '업데이트 날짜': updated_date,
            '사용자 UID': user_uid,
            '전화번호': user_info['phone'],
            '이메일': user_info['email'],
            '사용자 이름': user_info['name'],
            '강의 제목': lecture_title,
            '발급 일시': issued_str,
            'PDF URL': pdf_url
        }])
        
        df = pd.concat([df, new_row], ignore_index=True)
        
        logger.info(f"✅ 수료증 처리 완료: {user_uid[:8]}.../{cert_id[:8]}... - {lecture_title[:30]}...")
        return True, df
        
    except Exception as e:
        logger.warning(f"⚠️ 수료증 처리 실패 ({user_uid[:8]}.../{cert_id[:8]}...): {e}")
        
        try:
            cert_ref = db.collection('users').document(user_uid) \
                         .collection('completedCertificates').document(cert_id)
            cert_ref.update({
                'processingError': str(e)[:200],
                'errorOccurredAt': firestore.SERVER_TIMESTAMP,
                'retryCount': firestore.Increment(1)
            })
        except Exception:
            pass
        
        return False, df

def update_certificate_flags_batch(processed_certs, success=True):
    """배치 플래그 업데이트"""
    operation_id = f"update_flags_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        def update_single_flag(cert_info):
            user_uid, cert_id, cert_data = cert_info
            try:
                cert_ref = db.collection('users').document(user_uid) \
                             .collection('completedCertificates').document(cert_id)
                
                if success:
                    update_data = {
                        'excelUpdated': True,
                        'processedAt': firestore.SERVER_TIMESTAMP,
                        'processedBy': 'no_index_worker_v1',
                        'workerProcessed': True
                    }
                    
                    if 'processingError' in cert_data:
                        update_data['processingError'] = firestore.DELETE_FIELD
                    if 'readyForExcel' in cert_data:
                        update_data['readyForExcel'] = False
                    
                    cert_ref.update(update_data)
                    return "✅"
                    
                else:
                    cert_ref.update({
                        'excelSaveError': True,
                        'excelSaveErrorAt': firestore.SERVER_TIMESTAMP,
                        'retryCount': firestore.Increment(1)
                    })
                    return "⚠️"
                    
            except Exception as e:
                logger.debug(f"플래그 업데이트 실패: {e}")
                return "❌"
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(update_single_flag, processed_certs))
        
        success_count = sum(1 for r in results if r == '✅')
        logger.info(f"✅ 플래그 업데이트: {success_count}/{len(results)}")
        
    except Exception as e:
        logger.error(f"❌ 배치 플래그 업데이트 실패: {e}")
    finally:
        log_operation_end(operation_id)

# ===================================================================
# 배치 처리
# ===================================================================
def process_batch():
    """배치 처리 실행"""
    operation_id = f"batch_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        batch_start_time = datetime.now(timezone.utc)
        
        # 인덱스 없이 수료증 조회
        pending_certs = get_pending_certificates_no_index(limit=BATCH_SIZE)
        
        if not pending_certs:
            logger.info("😴 처리할 수료증이 없습니다")
            return
        
        logger.info(f"🚀 {len(pending_certs)}개 수료증 배치 처리 시작")
        
        df = load_master_excel()
        original_row_count = len(df)
        
        success_count = 0
        error_count = 0
        processed_certs = []
        
        for i, (user_uid, cert_id, cert_data) in enumerate(pending_certs, 1):
            if shutdown_flag:
                logger.info("🛑 종료 플래그 감지, 배치 처리 중단")
                break
            
            if len(pending_certs) > 5 and i % max(1, len(pending_certs) // 5) == 0:
                progress = (i / len(pending_certs)) * 100
                logger.info(f"📊 처리 진행률: {progress:.0f}% ({i}/{len(pending_certs)})")
            
            success, df = process_certificate(user_uid, cert_id, cert_data, df)
            
            if success:
                success_count += 1
                processed_certs.append((user_uid, cert_id, cert_data))
            else:
                error_count += 1
        
        if success_count > 0:
            new_row_count = len(df)
            logger.info(f"📊 Excel 저장: {original_row_count}행 → {new_row_count}행 (+{new_row_count - original_row_count})")
            
            excel_save_success = save_master_excel(df)
            update_certificate_flags_batch(processed_certs, success=excel_save_success)
            
            processing_time = (datetime.now(timezone.utc) - batch_start_time).total_seconds()
            
            if excel_save_success:
                logger.info(f"🎉 배치 처리 완료 - ✅성공: {success_count}, ❌실패: {error_count}, ⏱️시간: {processing_time:.1f}초")
            else:
                logger.error(f"❌ Excel 저장 실패 - 재시도 예정")
        else:
            logger.info(f"📊 배치 처리 완료 - 성공적으로 처리된 항목 없음 (❌실패: {error_count})")
        
    except Exception as e:
        logger.error(f"❌ 배치 처리 중 오류: {e}")
    finally:
        log_operation_end(operation_id)

def update_health_status():
    """헬스체크 파일 업데이트"""
    try:
        health_file = Path('/tmp/worker_healthy')
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'active_operations': len(current_operations),
            'poll_interval': POLL_INTERVAL_SECONDS,
            'batch_size': BATCH_SIZE,
            'mode': 'no_index'
        }
        
        with open(health_file, 'w') as f:
            json.dump(health_data, f)
            
    except Exception as e:
        logger.debug(f"헬스 파일 업데이트 실패: {e}")

# ===================================================================
# 메인 워커 루프
# ===================================================================
def run_worker():
    """메인 워커 루프"""
    logger.info(f"🚀 Certificate Worker v3.2 시작 (인덱스 없이 작동)")
    logger.info(f"⏱️ 폴링 간격: {POLL_INTERVAL_SECONDS}초")
    logger.info(f"📦 배치 크기: {BATCH_SIZE}")
    logger.info(f"🔄 최대 재시도: {MAX_RETRY_COUNT}")
    logger.info(f"🔧 재시도 리셋: {os.getenv('RESET_HIGH_RETRY_COUNT', 'false')}")
    logger.info(f"🔍 모드: 인덱스 없이 개별 사용자 검색")
    
    update_health_status()
    
    iteration = 0
    last_activity_time = None
    consecutive_empty_batches = 0
    
    while not shutdown_flag:
        try:
            iteration += 1
            
            if iteration % 10 == 0:
                update_health_status()
            
            batch_start_time = datetime.now(timezone.utc)
            
            if len(current_operations) > 5:
                logger.warning(f"⚠️ 너무 많은 동시 작업: {len(current_operations)}개")
                time.sleep(5)
                continue
            
            # 배치 처리
            process_batch()
            
            # 활동 감지 (간소화)
            consecutive_empty_batches += 1
            
            # 상태 로깅
            if iteration % 10 == 0:
                logger.info(f"📈 상태 - 반복: {iteration}, 활성작업: {len(current_operations)}개, 모드: 인덱스없음")
            
            # 동적 대기 시간
            if consecutive_empty_batches > 10:
                sleep_time = min(POLL_INTERVAL_SECONDS * 2, 120)
            else:
                sleep_time = POLL_INTERVAL_SECONDS
            
            # 인터럽트 가능한 대기
            for _ in range(sleep_time):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ 키보드 인터럽트 - 종료")
            break
        except Exception as e:
            logger.error(f"❌ 워커 루프 오류: {e}")
            time.sleep(min(POLL_INTERVAL_SECONDS, 60))
    
    # 종료 정리
    with operations_lock:
        if current_operations:
            logger.info(f"🔄 {len(current_operations)}개 작업 완료 대기...")
            for _ in range(30):
                if not current_operations:
                    break
                time.sleep(1)
    
    logger.info("👋 Certificate Worker 안전 종료 완료")

# ===================================================================
# 엔트리 포인트
# ===================================================================
if __name__ == "__main__":
    try:
        logger.info("🔍 최종 검증 중...")
        
        # Firebase 연결 테스트
        test_collection = db.collection('_worker_health')
        test_doc = test_collection.document(f"no_index_test_{int(time.time())}")
        test_doc.set({
            'timestamp': firestore.SERVER_TIMESTAMP,
            'worker': 'no_index_certificate_worker_v1',
            'startup_time': datetime.now(timezone.utc).isoformat(),
            'mode': 'no_index'
        })
        test_doc.delete()
        
        logger.info("✅ 최종 검증 완료")
        
        # 워커 시작
        run_worker()
        
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)