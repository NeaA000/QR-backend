# worker/certificate_worker.py - 수료증 처리 전용 워커 (재시도 로직 포함)

import os
import io
import time
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.oauth2 import service_account

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
# 환경변수 및 설정
# ===================================================================
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
MASTER_FILENAME = "master_certificates.xlsx"

# ===================================================================
# Firebase 초기화
# ===================================================================
def initialize_firebase():
    """Firebase Admin SDK 초기화"""
    try:
        if not firebase_admin._apps:
            # Railway 환경변수에서 자격증명 생성
            firebase_creds = {
                "type": os.environ.get("type", "service_account"),
                "project_id": os.environ["project_id"],
                "private_key_id": os.environ.get("private_key_id", ""),
                "private_key": os.environ["private_key"].replace('\\n', '\n'),
                "client_email": os.environ["client_email"],
                "client_id": os.environ.get("client_id", ""),
                "auth_uri": os.environ.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
                "token_uri": os.environ.get("token_uri", "https://oauth2.googleapis.com/token"),
                "auth_provider_x509_cert_url": os.environ.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
                "client_x509_cert_url": os.environ.get("client_x509_cert_url", "")
            }
            
            cred = credentials.Certificate(firebase_creds)
            firebase_admin.initialize_app(cred, {
                'storageBucket': f"{os.environ['project_id']}.firebasestorage.app"
            })
            
        db = firestore.client()
        bucket = storage.bucket()
        
        logger.info(f"✅ Firebase 초기화 완료 - Project: {os.environ['project_id']}")
        return db, bucket
        
    except Exception as e:
        logger.error(f"❌ Firebase 초기화 실패: {e}")
        raise

# Firebase 클라이언트 초기화
db, bucket = initialize_firebase()

# ===================================================================
# 종료 시그널 처리
# ===================================================================
shutdown_flag = False

def signal_handler(signum, frame):
    """SIGINT/SIGTERM 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 받음 ({signum}). 안전하게 종료합니다...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================================================================
# 헬스체크
# ===================================================================
def update_health_status():
    """헬스체크 파일 업데이트"""
    try:
        with open('/tmp/worker_healthy', 'w') as f:
            f.write(f"healthy at {datetime.now(timezone.utc).isoformat()}")
    except Exception as e:
        logger.warning(f"헬스 파일 업데이트 실패: {e}")

# ===================================================================
# 수료증 처리 함수들
# ===================================================================
def get_pending_certificates(limit=50):
    """
    처리 대기 중인 수료증 조회 - 수정된 조건
    
    Returns:
        list: [(user_uid, cert_id, cert_data), ...]
    """
    try:
        # 수정된 쿼리: readyForExcel 조건 제거, excelUpdated=False이고 pdfUrl이 있는 문서만 조회
        query = db.collection_group('completedCertificates') \
                  .where('excelUpdated', '==', False) \
                  .limit(limit)
        
        results = []
        for doc in query.stream():
            try:
                data = doc.to_dict()
                
                # PDF URL 확인 (필수 조건)
                pdf_url = data.get('pdfUrl', '')
                if not pdf_url or pdf_url.strip() == '':
                    logger.debug(f"⚠️ 문서 {doc.id}에 PDF URL이 없습니다. 건너뜁니다.")
                    continue
                
                # lectureTitle 확인 (선택적이지만 로그용)
                lecture_title = data.get('lectureTitle', '제목없음')
                
                # 문서 경로에서 user_uid 추출
                path_parts = doc.reference.path.split('/')
                if len(path_parts) >= 4:
                    user_uid = path_parts[1]
                    cert_id = doc.id
                    results.append((user_uid, cert_id, data))
                    logger.debug(f"📋 발견: {user_uid}/{cert_id} - {lecture_title}")
                else:
                    logger.warning(f"⚠️ 잘못된 문서 경로: {doc.reference.path}")
                    
            except Exception as e:
                logger.error(f"❌ 문서 파싱 오류 {doc.id}: {e}")
                
        logger.info(f"📋 {len(results)}개의 처리 대기 수료증 발견")
        
        # 디버그: 찾은 수료증들 간단히 로그
        if results:
            for user_uid, cert_id, data in results[:3]:  # 처음 3개만
                logger.info(f"  → {user_uid[:8]}.../{cert_id[:8]}... - {data.get('lectureTitle', '제목없음')}")
            if len(results) > 3:
                logger.info(f"  ... 그 외 {len(results) - 3}개 더")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 수료증 조회 실패: {e}")
        return []

def get_user_info(user_uid):
    """사용자 정보 조회"""
    try:
        user_doc = db.collection('users').document(user_uid).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            user_info = {
                'name': user_data.get('name', ''),
                'phone': user_data.get('phone', ''),
                'email': user_data.get('email', '')
            }
            logger.debug(f"👤 사용자 정보 조회 완료: {user_uid[:8]}... - {user_info['name']}")
            return user_info
        else:
            logger.warning(f"⚠️ 사용자 문서 없음: {user_uid}")
            return {'name': '', 'phone': '', 'email': ''}
            
    except Exception as e:
        logger.error(f"❌ 사용자 정보 조회 실패 ({user_uid}): {e}")
        return {'name': '', 'phone': '', 'email': ''}

def get_or_create_master_excel():
    """마스터 엑셀 파일 가져오기 또는 생성 - 폴백 옵션 포함"""
    try:
        # 1) Firebase Storage 시도
        try:
            master_blob = bucket.blob(MASTER_FILENAME)
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer = io.BytesIO(existing_bytes)
            df = pd.read_excel(excel_buffer, engine='openpyxl')
            logger.info(f"📥 Firebase Storage에서 기존 마스터 엑셀 로드 완료 (행 수: {len(df)})")
            
            # 기존 DataFrame에서 불필요한 열 제거 (혹시 있다면)
            columns_to_remove = ['User UID', 'Lecture Title', 'Issued At']
            for col in columns_to_remove:
                if col in df.columns:
                    df = df.drop(columns=[col])
                    logger.debug(f"🗑️ 컬럼 제거: {col}")
            
            return df
            
        except Exception as firebase_error:
            logger.warning(f"⚠️ Firebase Storage 로드 실패: {firebase_error}")
            
            # 2) 로컬 임시 파일 확인
            local_path = f'/tmp/{MASTER_FILENAME}'
            if os.path.exists(local_path):
                try:
                    df = pd.read_excel(local_path, engine='openpyxl')
                    logger.info(f"📥 로컬 임시 파일에서 마스터 엑셀 로드 완료 (행 수: {len(df)})")
                    return df
                except Exception as local_error:
                    logger.warning(f"⚠️ 로컬 파일 로드 실패: {local_error}")
            
            # 3) 새 DataFrame 생성
            logger.info("📄 새 마스터 엑셀 파일 생성")
            df = pd.DataFrame(columns=[
                '업데이트 날짜',
                '사용자 UID',
                '전화번호',
                '이메일',
                '사용자 이름',
                '강의 제목',
                '발급 일시',
                'PDF URL'
            ])
            return df
            
    except Exception as e:
        logger.error(f"❌ 마스터 엑셀 처리 실패: {e}")
        raise

def save_master_excel(df):
    """마스터 엑셀 파일 저장 - 재시도 로직 포함"""
    max_retries = 3
    retry_delay = 5  # 초
    
    for attempt in range(max_retries):
        try:
            # DataFrame을 엑셀로 변환
            out_buffer = io.BytesIO()
            with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Certificates')
            out_buffer.seek(0)
            
            # Firebase Storage에 업로드
            master_blob = bucket.blob(MASTER_FILENAME)
            master_blob.upload_from_file(
                out_buffer,
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            logger.info(f"✅ Firebase Storage에 마스터 엑셀 저장 완료 (총 {len(df)}행, 시도: {attempt + 1}/{max_retries})")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Excel 저장 실패 (시도 {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                logger.info(f"🔄 {retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ 최대 재시도 횟수 초과. Excel 저장 포기.")
                
                # 로컬 백업 저장 시도
                try:
                    local_path = f'/tmp/{MASTER_FILENAME}'
                    out_buffer.seek(0)
                    with open(local_path, 'wb') as f:
                        f.write(out_buffer.read())
                    
                    logger.info(f"💾 로컬 백업 저장 완료: {local_path}")
                    logger.info("📢 관리자: Firebase Storage 문제를 해결하고 워커를 재시작하세요!")
                except Exception as backup_error:
                    logger.error(f"❌ 로컬 백업도 실패: {backup_error}")
                
                return False
    
    return False

def process_certificate(user_uid, cert_id, cert_data, df):
    """
    단일 수료증 처리 - 실패 시 플래그 롤백 포함
    
    Args:
        user_uid: 사용자 UID
        cert_id: 수료증 ID
        cert_data: 수료증 데이터
        df: 마스터 DataFrame
        
    Returns:
        tuple: (성공여부, 업데이트된 DataFrame)
    """
    try:
        # 사용자 정보 조회
        user_info = get_user_info(user_uid)
        
        # 수료증 정보 추출
        lecture_title = cert_data.get('lectureTitle', cert_id)
        pdf_url = cert_data.get('pdfUrl', '')
        
        # 발급 시간 처리
        issued_at = cert_data.get('issuedAt')
        if hasattr(issued_at, 'to_datetime'):
            issued_str = issued_at.to_datetime().strftime('%Y-%m-%d %H:%M:%S')
        else:
            issued_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            logger.debug(f"⚠️ issuedAt 필드가 없어서 현재 시간 사용: {cert_id}")
        
        # 새 행 생성
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
        
        # DataFrame에 추가
        df = pd.concat([df, new_row], ignore_index=True)
        
        logger.info(f"✅ 수료증 데이터 처리 완료: {user_uid[:8]}.../{cert_id[:8]}... - {lecture_title}")
        return True, df
        
    except Exception as e:
        logger.error(f"❌ 수료증 처리 실패 ({user_uid[:8]}.../{cert_id[:8]}...): {e}")
        
        # 에러 기록 (플래그는 변경하지 않음)
        try:
            cert_ref = db.collection('users').document(user_uid) \
                         .collection('completedCertificates').document(cert_id)
            cert_ref.update({
                'processingError': str(e),
                'errorOccurredAt': firestore.SERVER_TIMESTAMP
            })
        except Exception as update_error:
            logger.error(f"❌ 에러 기록 실패: {update_error}")
            
        return False, df

def update_certificate_flags(processed_certs, success=True):
    """
    처리된 수료증들의 플래그를 일괄 업데이트
    
    Args:
        processed_certs: 처리된 수료증 리스트 [(user_uid, cert_id, cert_data), ...]
        success: Excel 저장 성공 여부
    """
    for user_uid, cert_id, cert_data in processed_certs:
        try:
            cert_ref = db.collection('users').document(user_uid) \
                         .collection('completedCertificates').document(cert_id)
            
            if success:
                # 성공 시: 완료 플래그 설정
                update_data = {
                    'excelUpdated': True,
                    'processedAt': firestore.SERVER_TIMESTAMP,
                    'processedBy': 'certificate_worker'
                }
                
                # readyForExcel 필드가 있다면 false로 설정
                if 'readyForExcel' in cert_data:
                    update_data['readyForExcel'] = False
                
                cert_ref.update(update_data)
                logger.debug(f"✅ 플래그 업데이트 완료: {user_uid[:8]}.../{cert_id[:8]}...")
                
            else:
                # 실패 시: 재시도를 위해 플래그 유지하고 에러 기록만
                cert_ref.update({
                    'excelSaveError': 'Excel 저장 실패 - 다음 주기에 재시도',
                    'excelSaveErrorAt': firestore.SERVER_TIMESTAMP,
                    'retryCount': firestore.Increment(1)
                })
                logger.warning(f"⚠️ 재시도 대상으로 유지: {user_uid[:8]}.../{cert_id[:8]}...")
                
        except Exception as e:
            logger.error(f"❌ 플래그 업데이트 실패 ({user_uid[:8]}.../{cert_id[:8]}...): {e}")

def process_batch():
    """배치 처리 실행 - 개선된 버전"""
    try:
        # 처리할 수료증 조회
        pending_certs = get_pending_certificates(limit=BATCH_SIZE)
        
        if not pending_certs:
            logger.debug("😴 처리할 수료증이 없습니다")
            return
        
        logger.info(f"🚀 {len(pending_certs)}개 수료증 처리 시작")
        
        # 마스터 엑셀 로드
        df = get_or_create_master_excel()
        original_row_count = len(df)
        
        # 처리 통계
        success_count = 0
        error_count = 0
        processed_certs = []  # 성공적으로 처리된 수료증 목록
        
        # 각 수료증 처리 (Excel 저장은 아직 안 함)
        for i, (user_uid, cert_id, cert_data) in enumerate(pending_certs, 1):
            if shutdown_flag:
                logger.info("🛑 종료 플래그 감지, 처리 중단")
                break
            
            logger.debug(f"📝 처리 중 ({i}/{len(pending_certs)}): {cert_id[:8]}...")
            success, df = process_certificate(user_uid, cert_id, cert_data, df)
            
            if success:
                success_count += 1
                processed_certs.append((user_uid, cert_id, cert_data))
            else:
                error_count += 1
        
        # Excel 저장 시도 (성공한 것들만)
        if success_count > 0:
            new_row_count = len(df)
            logger.info(f"📊 Excel 저장 시도: {original_row_count}행 → {new_row_count}행 (+{new_row_count - original_row_count})")
            
            excel_save_success = save_master_excel(df)
            
            if excel_save_success:
                # Excel 저장 성공 → 모든 처리된 수료증의 플래그 업데이트
                update_certificate_flags(processed_certs, success=True)
                logger.info(f"🎉 배치 처리 완료 - ✅성공: {success_count}, ❌실패: {error_count}")
                
            else:
                # Excel 저장 실패 → 플래그 롤백 (재시도 가능하도록)
                update_certificate_flags(processed_certs, success=False)
                logger.error(f"❌ Excel 저장 실패 - 수료증들이 다음 주기에 재시도됩니다")
                logger.info(f"📊 배치 처리 - 데이터 처리: {success_count}, Excel 저장: 실패, 기타 실패: {error_count}")
        else:
            logger.info(f"📊 배치 처리 완료 - 처리된 항목 없음 (❌실패: {error_count})")
        
    except Exception as e:
        logger.error(f"❌ 배치 처리 중 오류: {e}")

def get_statistics():
    """현재 통계 정보 조회"""
    try:
        # 처리 대기 중인 수료증 수
        pending_count = len(get_pending_certificates(limit=100))
        
        # 전체 수료증 수 (대략적)
        total_query = db.collection_group('completedCertificates').limit(1000)
        total_count = len(list(total_query.stream()))
        
        # 처리 완료된 수료증 수
        processed_query = db.collection_group('completedCertificates') \
                           .where('excelUpdated', '==', True).limit(1000)
        processed_count = len(list(processed_query.stream()))
        
        return {
            'pending': pending_count,
            'processed': processed_count,
            'total': total_count
        }
    except Exception as e:
        logger.error(f"❌ 통계 조회 실패: {e}")
        return {'pending': -1, 'processed': -1, 'total': -1}

# ===================================================================
# 메인 루프
# ===================================================================
def run_worker():
    """메인 워커 루프"""
    logger.info(f"🚀 Certificate Worker 시작")
    logger.info(f"⏱️ 폴링 간격: {POLL_INTERVAL_SECONDS}초")
    logger.info(f"📦 배치 크기: {BATCH_SIZE}")
    logger.info(f"🗂️ 마스터 파일: {MASTER_FILENAME}")
    
    # 초기 헬스 상태 및 통계
    update_health_status()
    
    # 시작 시 통계 표시
    initial_stats = get_statistics()
    logger.info(f"📊 초기 통계 - 대기: {initial_stats['pending']}, 처리완료: {initial_stats['processed']}, 전체: {initial_stats['total']}")
    
    iteration = 0
    last_activity_time = None
    
    while not shutdown_flag:
        try:
            iteration += 1
            logger.debug(f"🔄 반복 #{iteration}")
            
            # 헬스체크 업데이트
            update_health_status()
            
            # 배치 처리 실행
            batch_start_time = datetime.now(timezone.utc)
            process_batch()
            
            # 실제 처리가 있었다면 활동 시간 업데이트
            current_stats = get_statistics()
            if current_stats['pending'] != initial_stats.get('pending', -1):
                last_activity_time = batch_start_time
                initial_stats = current_stats  # 통계 업데이트
            
            # 10번째 반복마다 상태 로그
            if iteration % 10 == 0:
                stats = get_statistics()
                logger.info(f"📈 상태 - 반복: {iteration}, 대기: {stats['pending']}, 처리완료: {stats['processed']}")
                if last_activity_time:
                    logger.info(f"🕐 마지막 활동: {last_activity_time.strftime('%H:%M:%S')}")
                else:
                    logger.info("🕐 마지막 활동: 없음")
            
            # 대기 (1초씩 나눠서 종료 시그널 체크)
            for _ in range(POLL_INTERVAL_SECONDS):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ 키보드 인터럽트")
            break
        except Exception as e:
            logger.error(f"❌ 워커 루프 오류: {e}")
            time.sleep(min(POLL_INTERVAL_SECONDS, 30))
    
    logger.info("👋 Certificate Worker 종료")

# ===================================================================
# 엔트리 포인트
# ===================================================================
if __name__ == "__main__":
    try:
        run_worker()
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)