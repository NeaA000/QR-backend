# worker/realtime_certificate_worker.py - 백엔드 수정 없이 실시간 처리

import os
import io
import time
import logging
import signal
import sys
import threading
from datetime import datetime, timedelta
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
from concurrent.futures import ThreadPoolExecutor
import queue

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
logger = logging.getLogger('RealtimeCertificateWorker')

# ===================================================================
# 환경변수 및 설정
# ===================================================================
REALTIME_POLL_INTERVAL = int(os.getenv('REALTIME_POLL_INTERVAL', '5'))  # 5초마다 체크
BACKGROUND_POLL_INTERVAL = int(os.getenv('BACKGROUND_POLL_INTERVAL', '120'))  # 2분마다 체크
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
MASTER_FILENAME = "master_certificates.xlsx"

# ===================================================================
# Firebase 초기화
# ===================================================================
def initialize_firebase():
    """Firebase Admin SDK 초기화"""
    try:
        if not firebase_admin._apps:
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
                'storageBucket': f"{os.environ['project_id']}.appspot.com"
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
# 전역 변수
# ===================================================================
shutdown_flag = False
processing_queue = queue.Queue()
realtime_stats = {
    'processed_realtime': 0,
    'processed_background': 0,
    'failed': 0,
    'last_activity': None
}

def signal_handler(signum, frame):
    """종료 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 받음 ({signum}). 안전하게 종료합니다...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================================================================
# 수료증 처리 핵심 함수들
# ===================================================================

def update_health_status():
    """헬스체크 파일 업데이트"""
    try:
        with open('/tmp/worker_healthy', 'w') as f:
            f.write(f"healthy at {datetime.utcnow().isoformat()}")
    except Exception as e:
        logger.warning(f"헬스 파일 업데이트 실패: {e}")

def is_valid_pdf_url(pdf_url):
    """PDF URL 유효성 검사"""
    if not pdf_url:
        return False, "PDF URL이 비어있음"
    
    if not isinstance(pdf_url, str):
        return False, f"PDF URL이 문자열이 아님: {type(pdf_url)}"
    
    pdf_url = pdf_url.strip()
    if not pdf_url:
        return False, "PDF URL이 공백만 포함"
    
    if not pdf_url.startswith('https://firebasestorage.googleapis.com'):
        return False, f"Firebase Storage URL이 아님: {pdf_url[:100]}..."
    
    return True, "유효함"

def get_fresh_certificates(limit=20):
    """
    최근 생성된 수료증 조회 (5분 이내)
    실시간 처리용
    """
    try:
        # 5분 이내 생성된 것들만
        recent_threshold = datetime.utcnow() - timedelta(minutes=5)
        
        query = db.collection_group('completedCertificates') \
                  .where('readyForExcel', '==', True) \
                  .where('excelUpdated', '==', False) \
                  .limit(limit * 2)  # 여유분 확보
        
        results = []
        for doc in query.stream():
            try:
                data = doc.to_dict()
                issued_at = data.get('issuedAt')
                
                # 최근 생성된 것인지 확인
                if hasattr(issued_at, 'to_datetime'):
                    issued_time = issued_at.to_datetime().replace(tzinfo=None)
                    if issued_time >= recent_threshold:
                        
                        # PDF URL 검증
                        pdf_url = data.get('pdfUrl', '')
                        is_valid, reason = is_valid_pdf_url(pdf_url)
                        
                        if is_valid:
                            path_parts = doc.reference.path.split('/')
                            if len(path_parts) >= 4:
                                user_uid = path_parts[1]
                                cert_id = doc.id
                                results.append((user_uid, cert_id, data, 'fresh'))
                                
                                if len(results) >= limit:
                                    break
                        else:
                            # 무효한 PDF URL은 에러 마킹
                            mark_as_error(doc.reference, f"PDF URL 검증 실패: {reason}")
                            
            except Exception as e:
                logger.error(f"문서 파싱 오류 {doc.id}: {e}")
        
        if results:
            logger.info(f"🚀 실시간 처리 대상: {len(results)}개")
        return results
        
    except Exception as e:
        logger.error(f"❌ 최신 수료증 조회 실패: {e}")
        return []

def get_old_pending_certificates(limit=30):
    """
    오래된 미처리 수료증 조회 (5분 이상)
    백그라운드 처리용
    """
    try:
        old_threshold = datetime.utcnow() - timedelta(minutes=5)
        
        query = db.collection_group('completedCertificates') \
                  .where('readyForExcel', '==', True) \
                  .where('excelUpdated', '==', False) \
                  .limit(limit * 2)
        
        results = []
        for doc in query.stream():
            try:
                data = doc.to_dict()
                issued_at = data.get('issuedAt')
                
                # 오래된 것인지 확인
                if hasattr(issued_at, 'to_datetime'):
                    issued_time = issued_at.to_datetime().replace(tzinfo=None)
                    if issued_time < old_threshold:
                        
                        # PDF URL 검증
                        pdf_url = data.get('pdfUrl', '')
                        is_valid, reason = is_valid_pdf_url(pdf_url)
                        
                        if is_valid:
                            path_parts = doc.reference.path.split('/')
                            if len(path_parts) >= 4:
                                user_uid = path_parts[1]
                                cert_id = doc.id
                                results.append((user_uid, cert_id, data, 'old'))
                                
                                if len(results) >= limit:
                                    break
                        else:
                            mark_as_error(doc.reference, f"PDF URL 검증 실패: {reason}")
                            
            except Exception as e:
                logger.error(f"문서 파싱 오류 {doc.id}: {e}")
        
        if results:
            logger.info(f"🕰️ 백그라운드 처리 대상: {len(results)}개")
        return results
        
    except Exception as e:
        logger.error(f"❌ 오래된 수료증 조회 실패: {e}")
        return []

def get_user_info(user_uid):
    """사용자 정보 조회"""
    try:
        user_doc = db.collection('users').document(user_uid).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            return {
                'name': user_data.get('name', ''),
                'phone': user_data.get('phone', ''),
                'email': user_data.get('email', '')
            }
        else:
            logger.warning(f"사용자 문서 없음: {user_uid}")
            return {'name': '', 'phone': '', 'email': ''}
            
    except Exception as e:
        logger.error(f"사용자 정보 조회 실패 ({user_uid}): {e}")
        return {'name': '', 'phone': '', 'email': ''}

def get_or_create_master_excel():
    """마스터 엑셀 파일 가져오기 또는 생성 (스레드 안전)"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            master_blob = bucket.blob(MASTER_FILENAME)
            
            try:
                existing_bytes = master_blob.download_as_bytes()
                excel_buffer = io.BytesIO(existing_bytes)
                df = pd.read_excel(excel_buffer, engine='openpyxl')
                logger.debug(f"📥 기존 마스터 엑셀 로드 완료 (행 수: {len(df)})")
                
                # Cert ID 컬럼 확인
                if 'Cert ID' not in df.columns:
                    df['Cert ID'] = ""
                    
            except Exception:
                logger.info("📄 새 마스터 엑셀 파일 생성")
                df = pd.DataFrame(columns=[
                    '업데이트 날짜', '사용자 UID', '전화번호', '이메일',
                    '사용자 이름', '강의 제목', '발급 일시', 'PDF URL', 'Cert ID'
                ])
                
            return df
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.warning(f"⚠️ 엑셀 로드 실패 (시도 {attempt + 1}/{max_retries}), {wait_time}초 후 재시도: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ 마스터 엑셀 로드 최종 실패: {e}")
                raise

def save_master_excel(df):
    """마스터 엑셀 파일 저장 (재시도 포함)"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            out_buffer = io.BytesIO()
            with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Certificates')
            out_buffer.seek(0)
            
            master_blob = bucket.blob(MASTER_FILENAME)
            master_blob.upload_from_file(
                out_buffer,
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            logger.debug("✅ 마스터 엑셀 저장 완료")
            return True
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.warning(f"⚠️ 엑셀 저장 실패 (시도 {attempt + 1}/{max_retries}), {wait_time}초 후 재시도: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ 마스터 엑셀 저장 최종 실패: {e}")
                return False

def process_certificate_batch(certificates, process_type='realtime'):
    """
    수료증 배치 처리 (스레드 안전)
    """
    if not certificates:
        return 0, 0
    
    try:
        # 마스터 엑셀 로드 (한 번만)
        df = get_or_create_master_excel()
        initial_count = len(df)
        
        success_count = 0
        error_count = 0
        
        # 각 수료증 처리
        for user_uid, cert_id, cert_data, reason in certificates:
            if shutdown_flag:
                break
                
            try:
                # 중복 확인
                if not df.empty and 'Cert ID' in df.columns:
                    existing_cert_ids = df['Cert ID'].astype(str).values
                    if cert_id in existing_cert_ids:
                        logger.info(f"🔄 이미 엑셀에 존재: {cert_id}")
                        mark_as_processed(user_uid, cert_id, process_type)
                        success_count += 1
                        continue
                
                # 사용자 정보 조회
                user_info = get_user_info(user_uid)
                
                # 발급 시간 처리
                issued_at = cert_data.get('issuedAt')
                if hasattr(issued_at, 'to_datetime'):
                    issued_str = issued_at.to_datetime().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    issued_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                
                # 새 행 추가
                updated_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                new_row = pd.DataFrame([{
                    '업데이트 날짜': updated_date,
                    '사용자 UID': user_uid,
                    '전화번호': user_info['phone'],
                    '이메일': user_info['email'],
                    '사용자 이름': user_info['name'],
                    '강의 제목': cert_data.get('lectureTitle', cert_id),
                    '발급 일시': issued_str,
                    'PDF URL': cert_data.get('pdfUrl', ''),
                    'Cert ID': cert_id
                }])
                
                df = pd.concat([df, new_row], ignore_index=True)
                
                # Firestore 플래그 업데이트
                if mark_as_processed(user_uid, cert_id, process_type):
                    success_count += 1
                    logger.info(f"✅ {process_type} 처리 완료: {user_uid}/{cert_id}")
                else:
                    error_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.error(f"❌ 수료증 처리 실패 ({user_uid}/{cert_id}): {e}")
                mark_as_error_with_details(user_uid, cert_id, str(e))
        
        # 변경사항이 있으면 저장
        if success_count > 0:
            if save_master_excel(df):
                final_count = len(df)
                added_count = final_count - initial_count
                logger.info(f"📊 {process_type} 배치 완료 - 성공: {success_count}, 실패: {error_count}, 추가된 행: {added_count}")
            else:
                logger.error(f"❌ {process_type} 엑셀 저장 실패")
                return 0, success_count + error_count
        
        return success_count, error_count
        
    except Exception as e:
        logger.error(f"❌ {process_type} 배치 처리 중 오류: {e}")
        return 0, len(certificates)

def mark_as_processed(user_uid, cert_id, process_type):
    """Firestore 문서에 처리 완료 마킹"""
    try:
        cert_ref = db.collection('users').document(user_uid) \
                     .collection('completedCertificates').document(cert_id)
        
        update_data = {
            'excelUpdated': True,
            'readyForExcel': False,
            'processedAt': firestore.SERVER_TIMESTAMP,
            'processedBy': f'realtime_worker_{process_type}'
        }
        
        # 기존 에러 필드들 제거
        cert_snapshot = cert_ref.get()
        if cert_snapshot.exists:
            existing_data = cert_snapshot.to_dict()
            if existing_data.get('excelUpdateError'):
                update_data['excelUpdateError'] = firestore.DELETE_FIELD
            if existing_data.get('errorOccurredAt'):
                update_data['errorOccurredAt'] = firestore.DELETE_FIELD
        
        cert_ref.update(update_data)
        return True
        
    except Exception as e:
        logger.error(f"❌ 플래그 업데이트 실패 ({user_uid}/{cert_id}): {e}")
        return False

def mark_as_error(doc_ref, error_message):
    """문서에 에러 상태 마킹"""
    try:
        doc_ref.update({
            'excelUpdateError': error_message,
            'errorOccurredAt': firestore.SERVER_TIMESTAMP,
            'readyForExcel': False
        })
    except Exception as e:
        logger.error(f"에러 마킹 실패: {e}")

def mark_as_error_with_details(user_uid, cert_id, error_message):
    """상세 에러 마킹"""
    try:
        cert_ref = db.collection('users').document(user_uid) \
                     .collection('completedCertificates').document(cert_id)
        cert_ref.update({
            'excelUpdateError': error_message,
            'errorOccurredAt': firestore.SERVER_TIMESTAMP,
            'readyForExcel': False
        })
    except Exception as e:
        logger.error(f"상세 에러 마킹 실패 ({user_uid}/{cert_id}): {e}")

# ===================================================================
# 이중 모드 워커: 실시간 + 백그라운드
# ===================================================================

def realtime_worker():
    """실시간 워커 스레드 (5초마다 체크)"""
    logger.info("🚀 실시간 워커 스레드 시작")
    
    while not shutdown_flag:
        try:
            # 최근 생성된 수료증 조회
            fresh_certs = get_fresh_certificates(limit=BATCH_SIZE)
            
            if fresh_certs:
                success, failed = process_certificate_batch(fresh_certs, 'realtime')
                realtime_stats['processed_realtime'] += success
                realtime_stats['failed'] += failed
                realtime_stats['last_activity'] = datetime.utcnow()
            
            # 짧은 간격으로 대기
            for _ in range(REALTIME_POLL_INTERVAL):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ 실시간 워커 오류: {e}")
            time.sleep(10)
    
    logger.info("👋 실시간 워커 스레드 종료")

def background_worker():
    """백그라운드 워커 스레드 (2분마다 체크)"""
    logger.info("🕰️ 백그라운드 워커 스레드 시작")
    
    while not shutdown_flag:
        try:
            # 오래된 미처리 수료증 조회
            old_certs = get_old_pending_certificates(limit=BATCH_SIZE)
            
            if old_certs:
                success, failed = process_certificate_batch(old_certs, 'background')
                realtime_stats['processed_background'] += success
                realtime_stats['failed'] += failed
                realtime_stats['last_activity'] = datetime.utcnow()
            
            # 긴 간격으로 대기
            for _ in range(BACKGROUND_POLL_INTERVAL):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ 백그라운드 워커 오류: {e}")
            time.sleep(30)
    
    logger.info("👋 백그라운드 워커 스레드 종료")

def statistics_worker():
    """통계 로깅 스레드 (10분마다)"""
    logger.info("📊 통계 워커 스레드 시작")
    
    while not shutdown_flag:
        try:
            # 10분마다 통계 로깅
            for _ in range(600):  # 10분 = 600초
                if shutdown_flag:
                    break
                time.sleep(1)
            
            if not shutdown_flag:
                logger.info(f"📈 누적 통계:")
                logger.info(f"  🚀 실시간 처리: {realtime_stats['processed_realtime']}")
                logger.info(f"  🕰️ 백그라운드 처리: {realtime_stats['processed_background']}")
                logger.info(f"  ❌ 실패: {realtime_stats['failed']}")
                logger.info(f"  🕐 마지막 활동: {realtime_stats['last_activity']}")
                
        except Exception as e:
            logger.error(f"❌ 통계 워커 오류: {e}")
    
    logger.info("👋 통계 워커 스레드 종료")

# ===================================================================
# 메인 함수
# ===================================================================

def run_dual_mode_worker():
    """이중 모드 워커 실행"""
    logger.info("🚀 실시간 수료증 워커 시작")
    logger.info(f"   ⚡ 실시간 체크: {REALTIME_POLL_INTERVAL}초마다")
    logger.info(f"   🕰️ 백그라운드 체크: {BACKGROUND_POLL_INTERVAL}초마다")
    logger.info(f"   📦 배치 크기: {BATCH_SIZE}")
    
    # 초기 헬스 상태
    update_health_status()
    
    # 스레드 풀 생성
    threads = []
    
    try:
        # 실시간 워커 스레드
        realtime_thread = threading.Thread(target=realtime_worker, daemon=True)
        realtime_thread.start()
        threads.append(realtime_thread)
        
        # 백그라운드 워커 스레드
        background_thread = threading.Thread(target=background_worker, daemon=True)
        background_thread.start()
        threads.append(background_thread)
        
        # 통계 워커 스레드
        stats_thread = threading.Thread(target=statistics_worker, daemon=True)
        stats_thread.start()
        threads.append(stats_thread)
        
        # 메인 루프 (헬스체크만)
        while not shutdown_flag:
            update_health_status()
            time.sleep(30)  # 30초마다 헬스체크
            
    except KeyboardInterrupt:
        logger.info("⌨️ 키보드 인터럽트")
    except Exception as e:
        logger.error(f"❌ 메인 워커 오류: {e}")
    finally:
        # 모든 스레드 종료 대기
        logger.info("🛑 모든 워커 스레드 종료 대기 중...")
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=10)
        
        logger.info("🏁 실시간 수료증 워커 종료 완료")
        logger.info(f"📊 최종 통계:")
        logger.info(f"   🚀 실시간 처리: {realtime_stats['processed_realtime']}")
        logger.info(f"   🕰️ 백그라운드 처리: {realtime_stats['processed_background']}")
        logger.info(f"   ❌ 총 실패: {realtime_stats['failed']}")

# ===================================================================
# 엔트리 포인트
# ===================================================================

if __name__ == "__main__":
    try:
        run_dual_mode_worker()
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)