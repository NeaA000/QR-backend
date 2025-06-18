# worker/certificate_worker.py - Firebase Storage 버킷 문제 해결

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
BATCH_SIZE = min(20, int(os.getenv('BATCH_SIZE', '15')))
MASTER_FILENAME = "master_certificates.xlsx"
MAX_RETRY_COUNT = int(os.getenv('MAX_RETRY_COUNT', '5'))
HEALTH_CHECK_INTERVAL = 300

# 🔧 Firebase Storage 버킷 이름 결정
FIREBASE_STORAGE_BUCKET = os.getenv('FIREBASE_STORAGE_BUCKET')
if not FIREBASE_STORAGE_BUCKET:
    project_id = os.environ['project_id']
    # 새로운 Firebase는 .firebasestorage.app 형식 사용
    FIREBASE_STORAGE_BUCKET = f"{project_id}.firebasestorage.app"

logger.info(f"🚀 설정 완료 - 폴링:{POLL_INTERVAL_SECONDS}초, 배치:{BATCH_SIZE}개")
logger.info(f"🪣 Storage 버킷: {FIREBASE_STORAGE_BUCKET}")

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
        
        # 🔧 올바른 Storage 버킷 이름 사용
        app = firebase_admin.initialize_app(cred, {
            'storageBucket': FIREBASE_STORAGE_BUCKET
        })
        
        logger.info("✅ Firebase 초기화 완료")
        logger.info(f"✅ Firebase Storage 버킷: {FIREBASE_STORAGE_BUCKET}")
        
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
# 수료증 조회 함수들
# ===================================================================
def get_pending_certificates_debug(limit=50):
    """디버깅이 포함된 수료증 조회"""
    operation_id = f"get_pending_debug_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        logger.info("🔍 상세 디버깅 수료증 조회 시작")
        
        # 1단계: 전체 수료증 문서 수 확인
        try:
            all_certs_query = db.collection_group('completedCertificates').limit(100)
            all_docs = list(all_certs_query.stream())
            logger.info(f"📊 전체 수료증 문서: {len(all_docs)}개 발견")
            
            # 샘플 분석
            analysis = {
                'has_pdf_url': 0,
                'sent_to_admin_true': 0,
                'excel_updated_false': 0,
                'retry_over_limit': 0,
                'processable': 0
            }
            
            for doc in all_docs[:20]:
                try:
                    data = doc.to_dict()
                    
                    if data.get('pdfUrl', '').strip():
                        analysis['has_pdf_url'] += 1
                    
                    if data.get('sentToAdmin', False):
                        analysis['sent_to_admin_true'] += 1
                    
                    if not data.get('excelUpdated', True):
                        analysis['excel_updated_false'] += 1
                    
                    retry_count = data.get('retryCount', 0)
                    if retry_count >= MAX_RETRY_COUNT:
                        analysis['retry_over_limit'] += 1
                    
                    # 처리 가능한 문서 조건
                    if (data.get('pdfUrl', '').strip() and 
                        data.get('sentToAdmin', False) and 
                        not data.get('excelUpdated', True) and 
                        retry_count < MAX_RETRY_COUNT):
                        analysis['processable'] += 1
                        
                except Exception:
                    continue
            
            logger.info(f"📈 샘플 분석 결과 (상위 20개):")
            logger.info(f"  - PDF URL 있음: {analysis['has_pdf_url']}/20")
            logger.info(f"  - sentToAdmin=true: {analysis['sent_to_admin_true']}/20")
            logger.info(f"  - excelUpdated=false: {analysis['excel_updated_false']}/20")
            logger.info(f"  - 재시도 한도 초과: {analysis['retry_over_limit']}/20")
            logger.info(f"  - 처리 가능한 문서: {analysis['processable']}/20")
            
        except Exception as e:
            logger.warning(f"⚠️ 전체 분석 실패: {e}")
        
        # 2단계: 실제 쿼리 실행
        logger.info("🔍 실제 필터링 쿼리 실행")
        
        queries = [
            ("기본", db.collection_group('completedCertificates').limit(limit)),
            ("excel미완료", db.collection_group('completedCertificates').where('excelUpdated', '==', False).limit(limit)),
            ("전송완료", db.collection_group('completedCertificates').where('sentToAdmin', '==', True).limit(limit)),
        ]
        
        for name, query in queries:
            try:
                docs = list(query.stream())
                logger.info(f"  📋 {name}: {len(docs)}개 문서")
            except Exception as e:
                logger.warning(f"  ❌ {name} 실패: {e}")
        
        # 3단계: 최종 처리 대상 수료증 조회
        logger.info("🔍 최종 처리 대상 조회")
        
        try:
            final_query = db.collection_group('completedCertificates') \
                           .where('sentToAdmin', '==', True) \
                           .where('excelUpdated', '==', False) \
                           .limit(limit)
            
            results = []
            skip_reasons = {
                'no_pdf_url': 0,
                'retry_exceeded': 0,
                'path_error': 0,
                'data_error': 0
            }
            
            processed_count = 0
            for doc in final_query.stream():
                if shutdown_flag or processed_count >= limit:
                    break
                    
                processed_count += 1
                
                try:
                    data = doc.to_dict()
                    
                    # PDF URL 체크
                    pdf_url = data.get('pdfUrl', '').strip()
                    if not pdf_url:
                        skip_reasons['no_pdf_url'] += 1
                        continue
                    
                    # 재시도 횟수 체크
                    retry_count = data.get('retryCount', 0)
                    if retry_count >= MAX_RETRY_COUNT:
                        skip_reasons['retry_exceeded'] += 1
                        # 재시도 카운터 리셋 옵션
                        reset_enabled = os.getenv('RESET_HIGH_RETRY_COUNT', 'false').lower() == 'true'
                        if reset_enabled and retry_count <= 10:
                            logger.info(f"🔄 재시도 카운터 리셋: {doc.id[:12]}... (현재: {retry_count})")
                            try:
                                doc.reference.update({
                                    'retryCount': 0,
                                    'resetAt': firestore.SERVER_TIMESTAMP,
                                    'resetReason': 'worker_reset'
                                })
                                # 리셋 후 계속 처리
                            except Exception as reset_err:
                                logger.debug(f"리셋 실패: {reset_err}")
                                continue
                        else:
                            continue
                    
                    # 경로에서 UID 추출
                    path_parts = doc.reference.path.split('/')
                    if len(path_parts) < 4:
                        skip_reasons['path_error'] += 1
                        continue
                    
                    user_uid = path_parts[1]
                    cert_id = doc.id
                    
                    if not user_uid or not cert_id:
                        skip_reasons['data_error'] += 1
                        continue
                    
                    results.append((user_uid, cert_id, data))
                    
                    if len(results) <= 3:
                        lecture_title = data.get('lectureTitle', '제목없음')
                        logger.info(f"✅ 처리 대상: {user_uid[:8]}.../{cert_id[:8]}... - {lecture_title[:30]}...")
                        
                except Exception as e:
                    skip_reasons['data_error'] += 1
                    logger.debug(f"문서 처리 오류: {e}")
                    continue
            
            # 결과 로깅
            logger.info(f"🎯 최종 결과:")
            logger.info(f"  📊 검색된 문서: {processed_count}개")
            logger.info(f"  ✅ 처리 가능: {len(results)}개")
            logger.info(f"  ❌ 스킵된 이유:")
            for reason, count in skip_reasons.items():
                if count > 0:
                    reason_kr = {
                        'no_pdf_url': 'PDF URL 없음',
                        'retry_exceeded': '재시도 한도 초과',
                        'path_error': '경로 오류',
                        'data_error': '데이터 오류'
                    }.get(reason, reason)
                    logger.info(f"    - {reason_kr}: {count}개")
            
            if len(results) == 0 and processed_count > 0:
                logger.warning("⚠️ 문서는 있지만 처리 가능한 수료증이 없습니다!")
                logger.warning("💡 해결 방법:")
                logger.warning("  1. 앱에서 '관리자 전송' 버튼 클릭으로 pdfUrl 생성")
                logger.warning("  2. 환경변수 RESET_HIGH_RETRY_COUNT=true 설정으로 재시도 리셋")
                logger.warning("  3. Firebase에서 sentToAdmin=true 확인")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 최종 쿼리 실패: {e}")
            return []
            
    except Exception as e:
        logger.error(f"❌ 수료증 조회 실패: {e}")
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
        # 🔧 올바른 버킷으로 Firebase Storage에서 로드
        try:
            logger.info(f"📥 Firebase Storage에서 엑셀 로드 시도: {FIREBASE_STORAGE_BUCKET}/{MASTER_FILENAME}")
            master_blob = bucket.blob(MASTER_FILENAME)
            
            # 파일 존재 확인
            if not master_blob.exists():
                logger.info("⚠️ 마스터 엑셀 파일이 존재하지 않음, 새로 생성")
                return create_empty_dataframe()
            
            existing_bytes = master_blob.download_as_bytes()
            logger.info(f"📥 파일 다운로드 완료: {len(existing_bytes)} bytes")
            
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
            
        except Exception as firebase_error:
            logger.warning(f"⚠️ Firebase Storage 로드 실패: {firebase_error}")
            logger.info("⚠️ 새 파일 생성")
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
        
        # 로컬 백업
        backup_path = Path(f'/tmp/backup_{int(time.time())}.xlsx')
        try:
            with open(backup_path, 'wb') as f:
                f.write(out_buffer.getvalue())
            logger.info("💾 로컬 백업 완료")
        except Exception:
            logger.debug("로컬 백업 실패")
        
        # 🔧 올바른 버킷으로 Firebase Storage 업로드
        max_retries = 3
        for attempt in range(max_retries):
            try:
                out_buffer.seek(0)
                logger.info(f"📤 Firebase Storage 업로드 시도 {attempt + 1}/{max_retries}: {FIREBASE_STORAGE_BUCKET}/{MASTER_FILENAME}")
                
                master_blob = bucket.blob(MASTER_FILENAME)
                
                master_blob.upload_from_file(
                    out_buffer,
                    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                logger.info(f"✅ 엑셀 저장 완료 (총 {len(df)}행)")
                logger.info(f"✅ Firebase Storage 경로: {FIREBASE_STORAGE_BUCKET}/{MASTER_FILENAME}")
                
                # 백업 파일 정리
                try:
                    backup_path.unlink()
                except Exception:
                    pass
                
                return True
                
            except Exception as e:
                logger.warning(f"⚠️ 엑셀 저장 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if 'bucket does not exist' in str(e).lower():
                    logger.error(f"❌ Storage 버킷이 존재하지 않음: {FIREBASE_STORAGE_BUCKET}")
                    logger.error("💡 해결 방법:")
                    logger.error("   1. Firebase Console → Storage → Get started")
                    logger.error(f"   2. 환경변수 확인: FIREBASE_STORAGE_BUCKET={FIREBASE_STORAGE_BUCKET}")
                
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
                        'processedBy': 'fixed_storage_worker_v1',
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
        
        with ThreadPoolExecutor(max_workers=5) as executor:
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
        
        pending_certs = get_pending_certificates_debug(limit=BATCH_SIZE)
        
        if not pending_certs:
            logger.info("😴 처리할 수료증이 없습니다 - 상세 분석 완료")
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
            'storage_bucket': FIREBASE_STORAGE_BUCKET
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
    logger.info(f"🚀 Certificate Worker v3.3 시작 (Storage 버킷 문제 해결)")
    logger.info(f"⏱️ 폴링 간격: {POLL_INTERVAL_SECONDS}초")
    logger.info(f"📦 배치 크기: {BATCH_SIZE}")
    logger.info(f"🔄 최대 재시도: {MAX_RETRY_COUNT}")
    logger.info(f"🔧 재시도 리셋: {os.getenv('RESET_HIGH_RETRY_COUNT', 'false')}")
    logger.info(f"🪣 Storage 버킷: {FIREBASE_STORAGE_BUCKET}")
    
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
            
            if len(current_operations) > 10:
                logger.warning(f"⚠️ 너무 많은 동시 작업: {len(current_operations)}개")
                time.sleep(5)
                continue
            
            # 배치 처리
            process_batch()
            
            consecutive_empty_batches += 1
            
            # 상태 로깅
            if iteration % 10 == 0:
                logger.info(f"📈 상태 - 반복: {iteration}, 활성작업: {len(current_operations)}개, 버킷: {FIREBASE_STORAGE_BUCKET}")
            
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
        logger.info(f"🪣 사용할 Storage 버킷: {FIREBASE_STORAGE_BUCKET}")
        
        # Firebase 연결 테스트
        test_collection = db.collection('_worker_health')
        test_doc = test_collection.document(f"storage_test_{int(time.time())}")
        test_doc.set({
            'timestamp': firestore.SERVER_TIMESTAMP,
            'worker': 'fixed_storage_certificate_worker_v1',
            'startup_time': datetime.now(timezone.utc).isoformat(),
            'storage_bucket': FIREBASE_STORAGE_BUCKET
        })
        test_doc.delete()
        
        logger.info("✅ 최종 검증 완료")
        
        # 워커 시작
        run_worker()
        
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)