# worker/certificate_worker.py - 수료증 처리 전용 워커 (보안 및 성능 최적화)

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
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path

# ===================================================================
# 로깅 설정 (보안 강화)
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('CertificateWorker')

# 민감한 정보 로깅 방지
class SecurityFilter(logging.Filter):
    """민감한 정보를 필터링하는 로그 필터"""
    def filter(self, record):
        # 민감한 키워드가 포함된 로그 메시지 필터링
        sensitive_keywords = ['password', 'secret', 'key', 'token', 'credential']
        message = record.getMessage().lower()
        return not any(keyword in message for keyword in sensitive_keywords)

logger.addFilter(SecurityFilter())

# ===================================================================
# 환경변수 및 설정 (검증 강화)
# ===================================================================
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '30'))  # 배치 크기 감소로 메모리 효율성 향상
MASTER_FILENAME = "master_certificates.xlsx"
MAX_RETRY_COUNT = 3
HEALTH_CHECK_INTERVAL = 300  # 5분

# 필수 환경변수 검증
required_env_vars = [
    'type', 'project_id', 'private_key', 'client_email'
]

for var in required_env_vars:
    if not os.environ.get(var):
        logger.error(f"필수 환경변수 {var}가 설정되지 않았습니다.")
        sys.exit(1)

# ===================================================================
# Firebase 초기화 (보안 강화)
# ===================================================================
def initialize_firebase():
    """Firebase Admin SDK 안전 초기화"""
    try:
        if firebase_admin._apps:
            return db, bucket
            
        # 환경변수에서 자격증명 생성
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
        
        # 연결 테스트
        test_doc = db.collection('_health_check').document('test')
        test_doc.set({'timestamp': firestore.SERVER_TIMESTAMP})
        test_doc.delete()
        
        logger.info(f"✅ Firebase 초기화 및 연결 테스트 완료")
        return db, bucket
        
    except Exception as e:
        logger.error(f"❌ Firebase 초기화 실패: {e}")
        raise

# Firebase 클라이언트 초기화
db, bucket = initialize_firebase()

# ===================================================================
# 종료 시그널 처리 (안전한 종료)
# ===================================================================
shutdown_flag = False
current_operations = set()
operations_lock = threading.Lock()

def signal_handler(signum, frame):
    """SIGINT/SIGTERM 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 받음 ({signum}). 현재 작업 완료 후 안전하게 종료합니다...")
    shutdown_flag = True
    
    # 현재 진행 중인 작업 대기
    with operations_lock:
        if current_operations:
            logger.info(f"📋 {len(current_operations)}개 작업 완료 대기 중...")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================================================================
# 헬스체크 및 모니터링
# ===================================================================
def update_health_status():
    """헬스체크 파일 업데이트"""
    try:
        health_file = Path('/tmp/worker_healthy')
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'active_operations': len(current_operations),
            'last_batch_time': getattr(update_health_status, 'last_batch_time', None)
        }
        
        with open(health_file, 'w') as f:
            f.write(f"healthy at {health_data['timestamp']}")
            
    except Exception as e:
        logger.warning(f"헬스 파일 업데이트 실패: {e}")

def log_operation_start(operation_id):
    """작업 시작 로깅"""
    with operations_lock:
        current_operations.add(operation_id)

def log_operation_end(operation_id):
    """작업 종료 로깅"""
    with operations_lock:
        current_operations.discard(operation_id)

# ===================================================================
# 수료증 처리 함수들 (성능 최적화)
# ===================================================================
def get_pending_certificates(limit=50):
    """
    처리 대기 중인 수료증 조회 (성능 최적화)
    """
    operation_id = f"get_pending_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        # 인덱스 최적화된 쿼리 - pdfUrl 존재 여부도 확인
        query = db.collection_group('completedCertificates') \
                  .where('excelUpdated', '==', False) \
                  .limit(limit)
        
        results = []
        processed_count = 0
        
        for doc in query.stream():
            if shutdown_flag:
                break
                
            try:
                data = doc.to_dict()
                
                # PDF URL 필수 확인
                pdf_url = data.get('pdfUrl', '')
                if not pdf_url or pdf_url.strip() == '':
                    continue
                
                # 재시도 횟수 확인 (무한 재시도 방지)
                retry_count = data.get('retryCount', 0)
                if retry_count >= MAX_RETRY_COUNT:
                    logger.debug(f"⚠️ 최대 재시도 횟수 초과: {doc.id}")
                    continue
                
                # 문서 경로에서 user_uid 추출
                path_parts = doc.reference.path.split('/')
                if len(path_parts) >= 4:
                    user_uid = path_parts[1]
                    cert_id = doc.id
                    results.append((user_uid, cert_id, data))
                    processed_count += 1
                    
                    # 로그 최소화 (성능 향상)
                    if processed_count <= 3:
                        lecture_title = data.get('lectureTitle', '제목없음')
                        logger.debug(f"📋 발견: {user_uid[:8]}.../{cert_id[:8]}... - {lecture_title[:20]}...")
                else:
                    logger.warning(f"⚠️ 잘못된 문서 경로: {doc.reference.path}")
                    
            except Exception as e:
                logger.error(f"❌ 문서 파싱 오류 {doc.id}: {e}")
                
        if results:
            logger.info(f"📋 {len(results)}개의 처리 대기 수료증 발견")
            if len(results) > 3:
                logger.info(f"  ... 그 외 {len(results) - 3}개 더")
        else:
            logger.debug("😴 처리할 수료증이 없습니다")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 수료증 조회 실패: {e}")
        return []
    finally:
        log_operation_end(operation_id)

def get_user_info_safe(user_uid):
    """사용자 정보 안전 조회 (캐싱 포함)"""
    try:
        # 간단한 메모리 캐시 (워커 재시작 시 초기화됨)
        if not hasattr(get_user_info_safe, 'cache'):
            get_user_info_safe.cache = {}
        
        if user_uid in get_user_info_safe.cache:
            return get_user_info_safe.cache[user_uid]
        
        user_doc = db.collection('users').document(user_uid).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            user_info = {
                'name': user_data.get('name', ''),
                'phone': user_data.get('phone', ''),
                'email': user_data.get('email', '')
            }
            
            # 캐시 저장 (최대 1000개, LRU 방식)
            if len(get_user_info_safe.cache) < 1000:
                get_user_info_safe.cache[user_uid] = user_info
            
            logger.debug(f"👤 사용자 정보 조회: {user_uid[:8]}... - {user_info['name']}")
            return user_info
        else:
            empty_info = {'name': '', 'phone': '', 'email': ''}
            get_user_info_safe.cache[user_uid] = empty_info  # 빈 정보도 캐시
            logger.warning(f"⚠️ 사용자 문서 없음: {user_uid[:8]}...")
            return empty_info
            
    except Exception as e:
        logger.error(f"❌ 사용자 정보 조회 실패 ({user_uid[:8]}...): {e}")
        return {'name': '', 'phone': '', 'email': ''}

def get_or_create_master_excel():
    """마스터 엑셀 파일 가져오기 또는 생성 (백업 시스템 포함)"""
    operation_id = f"excel_load_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        # 1) Firebase Storage 시도
        try:
            master_blob = bucket.blob(MASTER_FILENAME)
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer = io.BytesIO(existing_bytes)
            df = pd.read_excel(excel_buffer, engine='openpyxl')
            
            # 데이터 검증
            expected_columns = ['업데이트 날짜', '사용자 UID', '전화번호', '이메일', '사용자 이름', '강의 제목', '발급 일시', 'PDF URL']
            if all(col in df.columns for col in expected_columns):
                logger.info(f"📥 Firebase Storage에서 마스터 엑셀 로드 완료 (행 수: {len(df)})")
                return df
            else:
                logger.warning("⚠️ 엑셀 파일의 컬럼 구조가 예상과 다름, 새로 생성")
                
        except Exception as firebase_error:
            logger.warning(f"⚠️ Firebase Storage 로드 실패: {firebase_error}")
        
        # 2) 로컬 백업 파일 확인
        local_backup_path = Path(f'/tmp/{MASTER_FILENAME}')
        if local_backup_path.exists():
            try:
                df = pd.read_excel(local_backup_path, engine='openpyxl')
                logger.info(f"📥 로컬 백업에서 마스터 엑셀 로드 완료 (행 수: {len(df)})")
                return df
            except Exception as local_error:
                logger.warning(f"⚠️ 로컬 백업 로드 실패: {local_error}")
        
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
    finally:
        log_operation_end(operation_id)

def save_master_excel_safe(df):
    """마스터 엑셀 파일 안전 저장 (백업 및 재시도 포함)"""
    operation_id = f"excel_save_{int(time.time())}"
    log_operation_start(operation_id)
    
    max_retries = 3
    retry_delay = 5
    
    try:
        # DataFrame을 엑셀로 변환
        out_buffer = io.BytesIO()
        with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Certificates')
        out_buffer.seek(0)
        
        # 로컬 백업 먼저 저장
        local_backup_path = Path(f'/tmp/{MASTER_FILENAME}')
        try:
            with open(local_backup_path, 'wb') as f:
                f.write(out_buffer.getvalue())
            logger.debug(f"💾 로컬 백업 저장 완료: {local_backup_path}")
        except Exception as backup_error:
            logger.warning(f"⚠️ 로컬 백업 저장 실패: {backup_error}")
        
        # Firebase Storage 업로드 (재시도 로직)
        for attempt in range(max_retries):
            try:
                out_buffer.seek(0)  # 버퍼 위치 초기화
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
                    logger.error(f"❌ 최대 재시도 횟수 초과. Firebase Storage 저장 실패")
                    logger.info("💾 로컬 백업은 보존됨. 관리자가 수동으로 업로드하세요.")
                    return False
        
        return False
        
    except Exception as e:
        logger.error(f"❌ Excel 저장 중 예외 발생: {e}")
        return False
    finally:
        log_operation_end(operation_id)

def process_certificate_safe(user_uid, cert_id, cert_data, df):
    """
    단일 수료증 안전 처리
    """
    operation_id = f"cert_process_{cert_id[:8]}"
    log_operation_start(operation_id)
    
    try:
        # 사용자 정보 조회
        user_info = get_user_info_safe(user_uid)
        
        # 수료증 정보 추출 및 검증
        lecture_title = cert_data.get('lectureTitle', cert_id)
        pdf_url = cert_data.get('pdfUrl', '')
        
        # PDF URL 재검증
        if not pdf_url or pdf_url.strip() == '':
            raise ValueError("PDF URL이 비어있습니다")
        
        # 발급 시간 처리
        issued_at = cert_data.get('issuedAt')
        if hasattr(issued_at, 'to_datetime'):
            issued_str = issued_at.to_datetime().strftime('%Y-%m-%d %H:%M:%S')
        else:
            issued_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            logger.debug(f"⚠️ issuedAt 필드 없음, 현재 시간 사용: {cert_id[:8]}...")
        
        # 중복 확인 (optional)
        existing_mask = (df['사용자 UID'] == user_uid) & (df['강의 제목'] == lecture_title)
        if existing_mask.any():
            logger.warning(f"⚠️ 중복 수료증 발견, 업데이트: {user_uid[:8]}.../{lecture_title[:20]}...")
            # 기존 행 업데이트 대신 새 행 추가 (이력 보존)
        
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
        
        logger.info(f"✅ 수료증 처리 완료: {user_uid[:8]}.../{cert_id[:8]}... - {lecture_title[:30]}...")
        return True, df
        
    except Exception as e:
        logger.error(f"❌ 수료증 처리 실패 ({user_uid[:8]}.../{cert_id[:8]}...): {e}")
        
        # 에러 기록 (플래그 변경 없이)
        try:
            cert_ref = db.collection('users').document(user_uid) \
                         .collection('completedCertificates').document(cert_id)
            cert_ref.update({
                'processingError': str(e)[:500],  # 에러 메시지 길이 제한
                'errorOccurredAt': firestore.SERVER_TIMESTAMP,
                'retryCount': firestore.Increment(1)
            })
        except Exception as update_error:
            logger.error(f"❌ 에러 기록 실패: {update_error}")
            
        return False, df
    finally:
        log_operation_end(operation_id)

def update_certificate_flags_batch(processed_certs, success=True):
    """
    처리된 수료증들의 플래그를 배치로 업데이트 (성능 최적화)
    """
    operation_id = f"flag_update_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        # ThreadPoolExecutor로 병렬 처리
        def update_single_flag(cert_info):
            user_uid, cert_id, cert_data = cert_info
            try:
                cert_ref = db.collection('users').document(user_uid) \
                             .collection('completedCertificates').document(cert_id)
                
                if success:
                    update_data = {
                        'excelUpdated': True,
                        'processedAt': firestore.SERVER_TIMESTAMP,
                        'processedBy': 'certificate_worker_v2'
                    }
                    
                    if 'readyForExcel' in cert_data:
                        update_data['readyForExcel'] = False
                    
                    # 에러 필드 정리
                    if 'processingError' in cert_data:
                        update_data['processingError'] = firestore.DELETE_FIELD
                    
                    cert_ref.update(update_data)
                    return f"✅ {user_uid[:8]}.../{cert_id[:8]}..."
                    
                else:
                    cert_ref.update({
                        'excelSaveError': 'Excel 저장 실패 - 재시도 예정',
                        'excelSaveErrorAt': firestore.SERVER_TIMESTAMP,
                        'retryCount': firestore.Increment(1)
                    })
                    return f"⚠️ {user_uid[:8]}.../{cert_id[:8]}..."
                    
            except Exception as e:
                logger.error(f"❌ 개별 플래그 업데이트 실패 ({user_uid[:8]}...): {e}")
                return f"❌ {user_uid[:8]}.../{cert_id[:8]}..."
        
        # 병렬 처리 (최대 5개 스레드)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(update_single_flag, processed_certs))
        
        success_count = sum(1 for r in results if r.startswith('✅'))
        total_count = len(results)
        
        if success:
            logger.info(f"✅ 플래그 업데이트 완료: {success_count}/{total_count}")
        else:
            logger.warning(f"⚠️ 재시도 대상 설정 완료: {success_count}/{total_count}")
            
    except Exception as e:
        logger.error(f"❌ 배치 플래그 업데이트 실패: {e}")
    finally:
        log_operation_end(operation_id)

def process_batch():
    """배치 처리 실행 (성능 및 안정성 최적화)"""
    operation_id = f"batch_{int(time.time())}"
    log_operation_start(operation_id)
    
    try:
        batch_start_time = datetime.now(timezone.utc)
        
        # 처리할 수료증 조회
        pending_certs = get_pending_certificates(limit=BATCH_SIZE)
        
        if not pending_certs:
            logger.debug("😴 처리할 수료증이 없습니다")
            return
        
        logger.info(f"🚀 {len(pending_certs)}개 수료증 배치 처리 시작")
        
        # 마스터 엑셀 로드
        df = get_or_create_master_excel()
        original_row_count = len(df)
        
        # 처리 통계
        success_count = 0
        error_count = 0
        processed_certs = []
        
        # 각 수료증 처리
        for i, (user_uid, cert_id, cert_data) in enumerate(pending_certs, 1):
            if shutdown_flag:
                logger.info("🛑 종료 플래그 감지, 배치 처리 중단")
                break
            
            # 진행률 로깅 (10% 단위)
            if i % max(1, len(pending_certs) // 10) == 0:
                progress = (i / len(pending_certs)) * 100
                logger.info(f"📊 처리 진행률: {progress:.0f}% ({i}/{len(pending_certs)})")
            
            success, df = process_certificate_safe(user_uid, cert_id, cert_data, df)
            
            if success:
                success_count += 1
                processed_certs.append((user_uid, cert_id, cert_data))
            else:
                error_count += 1
        
        # Excel 저장 및 플래그 업데이트
        if success_count > 0:
            new_row_count = len(df)
            logger.info(f"📊 Excel 저장 시도: {original_row_count}행 → {new_row_count}행 (+{new_row_count - original_row_count})")
            
            excel_save_success = save_master_excel_safe(df)
            update_certificate_flags_batch(processed_certs, success=excel_save_success)
            
            if excel_save_success:
                processing_time = (datetime.now(timezone.utc) - batch_start_time).total_seconds()
                logger.info(f"🎉 배치 처리 완료 - ✅성공: {success_count}, ❌실패: {error_count}, ⏱️시간: {processing_time:.1f}초")
            else:
                logger.error(f"❌ Excel 저장 실패 - 수료증들이 재시도됩니다")
                
            # 헬스체크 업데이트
            update_health_status.last_batch_time = batch_start_time.isoformat()
        else:
            logger.info(f"📊 배치 처리 완료 - 성공적으로 처리된 항목 없음 (❌실패: {error_count})")
        
    except Exception as e:
        logger.error(f"❌ 배치 처리 중 오류: {e}")
    finally:
        log_operation_end(operation_id)

def get_statistics():
    """현재 통계 정보 조회 (캐싱으로 성능 향상)"""
    try:
        # 간단한 캐싱 (30초)
        current_time = time.time()
        if hasattr(get_statistics, 'cache_time') and (current_time - get_statistics.cache_time) < 30:
            return get_statistics.cached_stats
        
        # 샘플링으로 성능 향상 (전체가 아닌 일부만 확인)
        sample_size = 500
        
        # 처리 대기 중인 수료증 수 (샘플)
        pending_query = db.collection_group('completedCertificates') \
                         .where('excelUpdated', '==', False).limit(sample_size)
        pending_count = len(list(pending_query.stream()))
        
        # 처리 완료된 수료증 수 (샘플)
        processed_query = db.collection_group('completedCertificates') \
                           .where('excelUpdated', '==', True).limit(sample_size)
        processed_count = len(list(processed_query.stream()))
        
        stats = {
            'pending': pending_count,
            'processed': processed_count,
            'total': pending_count + processed_count,
            'is_sample': True,
            'sample_size': sample_size
        }
        
        # 캐시 저장
        get_statistics.cached_stats = stats
        get_statistics.cache_time = current_time
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ 통계 조회 실패: {e}")
        return {'pending': -1, 'processed': -1, 'total': -1, 'error': str(e)}

# ===================================================================
# 메인 루프 (안정성 및 모니터링 강화)
# ===================================================================
def run_worker():
    """메인 워커 루프"""
    logger.info(f"🚀 Certificate Worker v2.0 시작 (플레이스토어 준수)")
    logger.info(f"⏱️ 폴링 간격: {POLL_INTERVAL_SECONDS}초")
    logger.info(f"📦 배치 크기: {BATCH_SIZE}")
    logger.info(f"🔄 최대 재시도: {MAX_RETRY_COUNT}")
    
    # 초기 설정
    update_health_status()
    
    # 시작 시 통계
    initial_stats = get_statistics()
    logger.info(f"📊 초기 통계 - 대기: {initial_stats['pending']}, 처리완료: {initial_stats['processed']}")
    
    iteration = 0
    last_activity_time = None
    consecutive_empty_batches = 0
    
    while not shutdown_flag:
        try:
            iteration += 1
            
            # 헬스체크 업데이트
            update_health_status()
            
            # 배치 처리 실행
            batch_start_time = datetime.now(timezone.utc)
            
            # 현재 작업 중인 항목 수 체크
            if len(current_operations) > 10:
                logger.warning(f"⚠️ 너무 많은 동시 작업: {len(current_operations)}개")
                time.sleep(5)
                continue
            
            # 이전 통계 저장
            prev_stats = get_statistics()
            
            # 배치 처리
            process_batch()
            
            # 처리 후 통계 확인
            current_stats = get_statistics()
            
            # 활동 감지
            if current_stats['pending'] != prev_stats['pending']:
                last_activity_time = batch_start_time
                consecutive_empty_batches = 0
            else:
                consecutive_empty_batches += 1
            
            # 상태 로깅 (적응적 주기)
            log_interval = 5 if consecutive_empty_batches > 5 else 10
            if iteration % log_interval == 0:
                stats = current_stats
                logger.info(f"📈 상태 - 반복: {iteration}, 대기: {stats['pending']}, 처리완료: {stats['processed']}")
                logger.info(f"🔧 활성 작업: {len(current_operations)}개")
                
                if last_activity_time:
                    idle_time = (datetime.now(timezone.utc) - last_activity_time).total_seconds()
                    logger.info(f"🕐 마지막 활동: {idle_time:.0f}초 전")
                else:
                    logger.info("🕐 마지막 활동: 없음")
            
            # 동적 대기 시간 (빈 배치가 연속으로 발생하면 대기 시간 증가)
            if consecutive_empty_batches > 3:
                sleep_time = min(POLL_INTERVAL_SECONDS * 2, 300)  # 최대 5분
                logger.debug(f"😴 연속 빈 배치 감지, 대기 시간 연장: {sleep_time}초")
            else:
                sleep_time = POLL_INTERVAL_SECONDS
            
            # 인터럽트 가능한 대기
            for _ in range(sleep_time):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ 키보드 인터럽트")
            break
        except Exception as e:
            logger.error(f"❌ 워커 루프 오류: {e}")
            error_sleep = min(POLL_INTERVAL_SECONDS, 60)
            logger.info(f"🔄 {error_sleep}초 후 재시작...")
            time.sleep(error_sleep)
    
    # 종료 시 정리
    with operations_lock:
        if current_operations:
            logger.info(f"🔄 {len(current_operations)}개 작업 완료 대기...")
            # 최대 30초 대기
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
        # 환경 검증
        logger.info("🔍 환경 검증 중...")
        
        # Firebase 연결 재확인
        test_collection = db.collection('_worker_health_check')
        test_doc = test_collection.document('test')
        test_doc.set({'timestamp': firestore.SERVER_TIMESTAMP, 'worker': 'certificate_worker_v2'})
        test_doc.delete()
        
        logger.info("✅ 환경 검증 완료")
        
        # 워커 시작
        run_worker()
        
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)