# worker/certificate_worker.py - 구글 플레이스토어 보안 정책 준수 버전

import os
import io
import time
import logging
import signal
import sys
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
import json
import ssl
import certifi

# ===================================================================
# 🔒 보안 강화 로깅 설정 (민감정보 완전 차단)
# ===================================================================
class SecureFormatter(logging.Formatter):
    """민감한 정보를 마스킹하는 보안 포매터"""
    
    SENSITIVE_PATTERNS = [
        'password', 'secret', 'key', 'token', 'credential', 'auth',
        'private', 'firebase', 'uid', 'email', 'phone'
    ]
    
    def format(self, record):
        # 로그 메시지에서 민감한 정보 마스킹
        msg = super().format(record)
        for pattern in self.SENSITIVE_PATTERNS:
            if pattern in msg.lower():
                # 민감한 키워드가 포함된 경우 해시로 대체
                msg_hash = hashlib.sha256(msg.encode()).hexdigest()[:8]
                return f"[SECURE_LOG_{msg_hash}] - 민감정보 포함으로 마스킹됨"
        return msg

# 보안 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 루트 로거에 보안 포매터 적용
for handler in logging.root.handlers:
    handler.setFormatter(SecureFormatter())

logger = logging.getLogger('SecureCertificateWorker')

# ===================================================================
# 🔒 환경변수 검증 및 보안 설정
# ===================================================================
class SecurityValidator:
    """보안 검증 클래스"""
    
    @staticmethod
    def validate_environment() -> bool:
        """환경변수 보안 검증"""
        required_vars = ['type', 'project_id', 'private_key', 'client_email']
        
        for var in required_vars:
            value = os.environ.get(var)
            if not value:
                logger.error(f"필수 환경변수 누락: {var}")
                return False
            
            # 환경변수 값 기본 검증
            if var == 'project_id' and not value.replace('-', '').replace('_', '').isalnum():
                logger.error(f"잘못된 project_id 형식")
                return False
                
            if var == 'client_email' and '@' not in value:
                logger.error(f"잘못된 client_email 형식")
                return False
        
        return True
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """파일명 보안 검증 및 정리"""
        # 위험한 문자 제거
        dangerous_chars = ['..', '/', '\\', '<', '>', ':', '"', '|', '?', '*']
        safe_name = filename
        
        for char in dangerous_chars:
            safe_name = safe_name.replace(char, '_')
        
        # 길이 제한
        if len(safe_name) > 100:
            safe_name = safe_name[:100]
        
        return safe_name
    
    @staticmethod
    def generate_secure_token() -> str:
        """보안 토큰 생성"""
        return secrets.token_urlsafe(32)

# 환경 검증
if not SecurityValidator.validate_environment():
    logger.error("🚨 보안 검증 실패 - 워커 종료")
    sys.exit(1)

# 설정값 (보안 강화)
POLL_INTERVAL_SECONDS = max(30, int(os.getenv('POLL_INTERVAL_SECONDS', '45')))  # 최소 30초
BATCH_SIZE = min(15, int(os.getenv('BATCH_SIZE', '10')))  # 최대 15개로 제한
MASTER_FILENAME = SecurityValidator.sanitize_filename("master_certificates_secure.xlsx")
MAX_RETRY_COUNT = 2  # 재시도 횟수 제한
HEALTH_CHECK_INTERVAL = 300
MAX_CONCURRENT_OPERATIONS = 3  # 동시 작업 수 제한

logger.info(f"🔒 보안 설정 완료 - 폴링:{POLL_INTERVAL_SECONDS}초, 배치:{BATCH_SIZE}개")

# ===================================================================
# 🔒 보안 강화된 Firebase 초기화
# ===================================================================
def initialize_firebase_secure() -> Tuple[Any, Any, Any]:
    """보안 강화된 Firebase 초기화"""
    try:
        if firebase_admin._apps:
            return firebase_admin.get_app(), firestore.client(), storage.bucket()
        
        # SSL 컨텍스트 보안 설정
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        
        # 환경변수에서 안전하게 자격증명 생성
        firebase_creds = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ.get("private_key_id", ""),
            "private_key": os.environ["private_key"].replace('\\n', '\n'),
            "client_email": os.environ["client_email"],
            "client_id": os.environ.get("client_id", ""),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",  # 하드코딩으로 보안 강화
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.environ.get("client_x509_cert_url", "")
        }
        
        # 자격증명 검증
        required_cred_fields = ["type", "project_id", "private_key", "client_email"]
        for field in required_cred_fields:
            if not firebase_creds.get(field):
                raise ValueError(f"Firebase 자격증명 필드 누락: {field}")
        
        cred = credentials.Certificate(firebase_creds)
        
        # 보안 강화된 스토리지 버킷 설정
        project_id = os.environ['project_id']
        if not project_id.replace('-', '').replace('_', '').isalnum():
            raise ValueError("잘못된 project_id 형식")
        
        storage_bucket = f"{project_id}.appspot.com"
        
        app = firebase_admin.initialize_app(cred, {
            'storageBucket': storage_bucket
        })
        
        db = firestore.client()
        bucket = storage.bucket()
        
        # 🔒 보안 연결 테스트 (민감정보 제외)
        test_token = SecurityValidator.generate_secure_token()
        test_doc_id = f"health_check_{test_token[:8]}"
        
        test_doc = db.collection('_worker_health').document(test_doc_id)
        test_doc.set({
            'timestamp': firestore.SERVER_TIMESTAMP,
            'worker_version': 'secure_v4.0',
            'security_level': 'enhanced',
            'test_token': test_token[:16]  # 일부만 저장
        })
        
        # 테스트 문서 즉시 삭제
        test_doc.delete()
        
        logger.info("✅ 보안 강화된 Firebase 초기화 완료")
        return app, db, bucket
        
    except Exception as e:
        logger.error(f"❌ Firebase 초기화 실패")
        # 보안상 상세 에러 정보는 로그에 남기지 않음
        raise Exception("Firebase 연결 실패")

# Firebase 클라이언트 초기화
try:
    app, db, bucket = initialize_firebase_secure()
except Exception as e:
    logger.error("🚨 Firebase 초기화 실패 - 워커 종료")
    sys.exit(1)

# ===================================================================
# 🔒 안전한 종료 및 운영 관리
# ===================================================================
shutdown_flag = False
current_operations: set = set()
operations_lock = threading.RLock()  # RLock으로 데드락 방지
operation_timeout = 300  # 5분 작업 타임아웃

def signal_handler(signum, frame):
    """안전한 종료 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 수신 - 안전한 종료 시작")
    shutdown_flag = True
    
    with operations_lock:
        if current_operations:
            logger.info(f"📋 {len(current_operations)}개 작업 완료 대기")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def manage_operation(operation_id: str):
    """작업 관리 컨텍스트 매니저"""
    class OperationManager:
        def __init__(self, op_id: str):
            self.op_id = op_id
            self.start_time = time.time()
        
        def __enter__(self):
            with operations_lock:
                current_operations.add(self.op_id)
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            with operations_lock:
                current_operations.discard(self.op_id)
            
            duration = time.time() - self.start_time
            if duration > operation_timeout:
                logger.warning(f"⚠️ 작업 시간 초과: {self.op_id} ({duration:.1f}초)")
    
    return OperationManager(operation_id)

# ===================================================================
# 🔒 보안 강화된 데이터 처리 함수들
# ===================================================================
def secure_collection_group_query(limit: int = 20) -> List[Tuple[str, str, Dict[str, Any]]]:
    """보안 강화된 수료증 조회"""
    operation_id = f"secure_query_{int(time.time())}"
    
    with manage_operation(operation_id):
        try:
            # 쿼리 제한 강화
            safe_limit = min(limit, MAX_CONCURRENT_OPERATIONS * 5)
            
            query = db.collection_group('completedCertificates') \
                      .where('excelUpdated', '==', False) \
                      .where('sentToAdmin', '==', True) \
                      .limit(safe_limit)
            
            results = []
            processed_count = 0
            
            for doc in query.stream():
                if shutdown_flag or processed_count >= safe_limit:
                    break
                
                processed_count += 1
                
                try:
                    data = doc.to_dict()
                    
                    # 🔒 데이터 보안 검증
                    if not secure_validate_certificate_data(data):
                        logger.debug(f"⚠️ 보안 검증 실패한 문서 스킵")
                        continue
                    
                    # PDF URL 필수 확인
                    pdf_url = data.get('pdfUrl', '').strip()
                    if not pdf_url or not pdf_url.startswith('https://'):
                        continue
                    
                    # 재시도 횟수 확인
                    retry_count = data.get('retryCount', 0)
                    if retry_count >= MAX_RETRY_COUNT:
                        continue
                    
                    # 문서 경로 보안 검증
                    path_parts = doc.reference.path.split('/')
                    if len(path_parts) >= 4:
                        user_uid = path_parts[1]
                        cert_id = doc.id
                        
                        # UID 형식 기본 검증
                        if secure_validate_uid(user_uid) and secure_validate_cert_id(cert_id):
                            results.append((user_uid, cert_id, data))
                            
                            if len(results) <= 2:  # 처음 2개만 로그
                                logger.info(f"📋 보안 검증된 수료증 발견")
                        
                except Exception as e:
                    logger.debug(f"⚠️ 문서 처리 중 오류")
                    continue
            
            logger.info(f"✅ 보안 쿼리 완료: {len(results)}개 수료증 발견")
            return results
            
        except Exception as e:
            logger.error(f"❌ 보안 쿼리 실패")
            return []

def secure_validate_certificate_data(data: Dict[str, Any]) -> bool:
    """수료증 데이터 보안 검증"""
    try:
        # 필수 필드 확인
        required_fields = ['videoId', 'pdfUrl', 'sentToAdmin']
        for field in required_fields:
            if field not in data:
                return False
        
        # URL 보안 검증
        pdf_url = data.get('pdfUrl', '')
        if not pdf_url.startswith('https://') or 'firebasestorage.googleapis.com' not in pdf_url:
            return False
        
        # sentToAdmin 플래그 확인
        if not data.get('sentToAdmin', False):
            return False
        
        return True
        
    except Exception:
        return False

def secure_validate_uid(uid: str) -> bool:
    """UID 보안 검증"""
    if not uid or len(uid) < 10 or len(uid) > 50:
        return False
    
    # 기본 문자열 검증 (알파벳, 숫자, 하이픈만 허용)
    import re
    if not re.match(r'^[a-zA-Z0-9\-_]+$', uid):
        return False
    
    return True

def secure_validate_cert_id(cert_id: str) -> bool:
    """수료증 ID 보안 검증"""
    if not cert_id or len(cert_id) < 5 or len(cert_id) > 100:
        return False
    
    # 기본 문자열 검증
    import re
    if not re.match(r'^[a-zA-Z0-9\-_]+$', cert_id):
        return False
    
    return True

def secure_get_user_info(user_uid: str) -> Dict[str, str]:
    """보안 강화된 사용자 정보 조회"""
    # 메모리 캐시 (보안 토큰 기반)
    if not hasattr(secure_get_user_info, 'cache'):
        secure_get_user_info.cache = {}
    
    # UID 해시를 캐시 키로 사용
    cache_key = hashlib.sha256(user_uid.encode()).hexdigest()[:16]
    
    if cache_key in secure_get_user_info.cache:
        return secure_get_user_info.cache[cache_key]
    
    try:
        user_doc = db.collection('users').document(user_uid).get()
        
        if user_doc.exists:
            user_data = user_doc.to_dict()
            
            # 🔒 민감정보 필터링
            safe_user_info = {
                'name': secure_sanitize_text(user_data.get('name', '')),
                'phone': secure_sanitize_phone(user_data.get('phone', '')),
                'email': secure_sanitize_email(user_data.get('email', ''))
            }
            
            # 캐시 저장 (최대 100개로 제한)
            if len(secure_get_user_info.cache) < 100:
                secure_get_user_info.cache[cache_key] = safe_user_info
            
            return safe_user_info
        else:
            empty_info = {'name': '', 'phone': '', 'email': ''}
            secure_get_user_info.cache[cache_key] = empty_info
            return empty_info
            
    except Exception as e:
        logger.debug(f"⚠️ 사용자 정보 조회 실패")
        return {'name': '', 'phone': '', 'email': ''}

def secure_sanitize_text(text: str) -> str:
    """텍스트 보안 정리"""
    if not text:
        return ''
    
    # 길이 제한
    text = text[:50]
    
    # 위험한 문자 제거
    import re
    text = re.sub(r'[<>"\']', '', text)
    
    return text.strip()

def secure_sanitize_phone(phone: str) -> str:
    """전화번호 보안 정리"""
    if not phone:
        return ''
    
    # 숫자와 하이픈만 허용
    import re
    phone = re.sub(r'[^\d\-]', '', phone)
    
    return phone[:20]

def secure_sanitize_email(email: str) -> str:
    """이메일 보안 정리"""
    if not email or '@' not in email:
        return ''
    
    # 기본 이메일 형식 검증
    import re
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return ''
    
    return email[:100]

# ===================================================================
# 🔒 보안 강화된 Excel 처리
# ===================================================================
def secure_load_master_excel() -> pd.DataFrame:
    """보안 강화된 마스터 엑셀 로드"""
    operation_id = f"secure_excel_load_{int(time.time())}"
    
    with manage_operation(operation_id):
        try:
            # Firebase Storage에서 로드 시도
            try:
                master_blob = bucket.blob(MASTER_FILENAME)
                
                # 파일 메타데이터 검증
                blob_metadata = master_blob.reload()
                if master_blob.size > 10 * 1024 * 1024:  # 10MB 제한
                    raise ValueError("파일 크기 초과")
                
                existing_bytes = master_blob.download_as_bytes()
                
                # 🔒 파일 무결성 검증
                if len(existing_bytes) < 100:  # 최소 크기 확인
                    raise ValueError("파일 크기 이상")
                
                excel_buffer = io.BytesIO(existing_bytes)
                df = pd.read_excel(excel_buffer, engine='openpyxl')
                
                # 🔒 데이터 구조 보안 검증
                expected_columns = [
                    '업데이트 날짜', '사용자 UID', '전화번호', '이메일', 
                    '사용자 이름', '강의 제목', '발급 일시', 'PDF URL'
                ]
                
                if not all(col in df.columns for col in expected_columns):
                    logger.warning("⚠️ 엑셀 컬럼 구조 이상, 새로 생성")
                    return create_secure_empty_dataframe()
                
                # 🔒 데이터 크기 제한 (최대 10,000행)
                if len(df) > 10000:
                    logger.warning("⚠️ 데이터 크기 제한으로 최근 5,000행만 유지")
                    df = df.tail(5000).reset_index(drop=True)
                
                logger.info(f"✅ 보안 검증된 엑셀 로드 완료 (행 수: {len(df)})")
                return df
                
            except Exception as firebase_error:
                logger.info("⚠️ Firebase Storage 로드 실패, 새 파일 생성")
                return create_secure_empty_dataframe()
            
        except Exception as e:
            logger.error(f"❌ 보안 엑셀 로드 실패")
            return create_secure_empty_dataframe()

def create_secure_empty_dataframe() -> pd.DataFrame:
    """보안 검증된 빈 DataFrame 생성"""
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

def secure_save_master_excel(df: pd.DataFrame) -> bool:
    """보안 강화된 엑셀 저장"""
    operation_id = f"secure_excel_save_{int(time.time())}"
    
    with manage_operation(operation_id):
        try:
            # 🔒 데이터 보안 검증
            if len(df) > 15000:  # 저장 크기 제한
                logger.warning("⚠️ 저장 크기 제한으로 최근 10,000행만 저장")
                df = df.tail(10000).reset_index(drop=True)
            
            # 민감정보 추가 정리
            df_clean = df.copy()
            for col in ['전화번호', '이메일', '사용자 이름']:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(lambda x: secure_sanitize_text(str(x)) if pd.notna(x) else '')
            
            # Excel 변환
            out_buffer = io.BytesIO()
            with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
                df_clean.to_excel(writer, index=False, sheet_name='Certificates')
            out_buffer.seek(0)
            
            # 🔒 로컬 백업 (보안 경로)
            secure_backup_path = Path(f'/tmp/secure_backup_{secrets.token_hex(8)}.xlsx')
            try:
                with open(secure_backup_path, 'wb') as f:
                    f.write(out_buffer.getvalue())
                logger.info("💾 보안 로컬 백업 완료")
            except Exception:
                logger.debug("⚠️ 로컬 백업 실패")
            
            # Firebase Storage 업로드 (재시도 포함)
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    out_buffer.seek(0)
                    master_blob = bucket.blob(MASTER_FILENAME)
                    
                    # 🔒 보안 메타데이터 설정
                    secure_metadata = {
                        'contentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        'metadata': {
                            'worker_version': 'secure_v4.0',
                            'updated_by': 'secure_worker',
                            'security_level': 'enhanced',
                            'row_count': str(len(df_clean))
                        }
                    }
                    
                    master_blob.upload_from_file(
                        out_buffer,
                        content_type=secure_metadata['contentType']
                    )
                    
                    logger.info(f"✅ 보안 엑셀 저장 완료 (총 {len(df_clean)}행)")
                    
                    # 백업 파일 정리
                    try:
                        secure_backup_path.unlink()
                    except Exception:
                        pass
                    
                    return True
                    
                except Exception as e:
                    logger.warning(f"⚠️ 엑셀 저장 실패 (시도 {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                    else:
                        logger.error("❌ 최대 재시도 초과")
                        return False
            
            return False
            
        except Exception as e:
            logger.error(f"❌ 보안 엑셀 저장 중 예외")
            return False

# ===================================================================
# 🔒 보안 강화된 수료증 처리
# ===================================================================
def secure_process_certificate(user_uid: str, cert_id: str, cert_data: Dict[str, Any], df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """보안 강화된 단일 수료증 처리"""
    operation_id = f"secure_cert_{cert_id[:8]}"
    
    with manage_operation(operation_id):
        try:
            # 🔒 입력 데이터 보안 검증
            if not secure_validate_uid(user_uid) or not secure_validate_cert_id(cert_id):
                raise ValueError("보안 검증 실패")
            
            if not secure_validate_certificate_data(cert_data):
                raise ValueError("수료증 데이터 검증 실패")
            
            # 사용자 정보 안전 조회
            user_info = secure_get_user_info(user_uid)
            
            # 수료증 정보 추출 및 정리
            lecture_title = secure_sanitize_text(cert_data.get('lectureTitle', cert_id))
            pdf_url = cert_data.get('pdfUrl', '').strip()
            
            if not pdf_url.startswith('https://'):
                raise ValueError("잘못된 PDF URL")
            
            # 발급 시간 처리
            issued_at = cert_data.get('issuedAt')
            if hasattr(issued_at, 'to_datetime'):
                issued_str = issued_at.to_datetime().strftime('%Y-%m-%d %H:%M:%S')
            else:
                issued_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            # 🔒 중복 확인 (보안 강화)
            existing_mask = (
                (df['사용자 UID'] == user_uid) & 
                (df['강의 제목'] == lecture_title) & 
                (df['PDF URL'] == pdf_url)
            )
            
            if existing_mask.any():
                logger.info("⚠️ 완전 중복 수료증 발견, 스킵")
                return True, df  # 중복이므로 성공으로 처리
            
            # 새 행 생성 (보안 검증된 데이터만)
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
            
            logger.info(f"✅ 보안 수료증 처리 완료")
            return True, df
            
        except Exception as e:
            logger.warning(f"⚠️ 수료증 처리 실패")
            
            # 🔒 에러 기록 (민감정보 제외)
            try:
                cert_ref = db.collection('users').document(user_uid) \
                             .collection('completedCertificates').document(cert_id)
                cert_ref.update({
                    'processingError': 'SECURE_ERROR_OCCURRED',
                    'errorOccurredAt': firestore.SERVER_TIMESTAMP,
                    'retryCount': firestore.Increment(1)
                })
            except Exception:
                pass
            
            return False, df

def secure_update_certificate_flags_batch(processed_certs: List[Tuple[str, str, Dict]], success: bool = True):
    """보안 강화된 배치 플래그 업데이트"""
    operation_id = f"secure_flag_update_{int(time.time())}"
    
    with manage_operation(operation_id):
        try:
            def update_single_flag_secure(cert_info):
                user_uid, cert_id, cert_data = cert_info
                try:
                    # 🔒 재검증
                    if not secure_validate_uid(user_uid) or not secure_validate_cert_id(cert_id):
                        return "❌ 보안검증실패"
                    
                    cert_ref = db.collection('users').document(user_uid) \
                                 .collection('completedCertificates').document(cert_id)
                    
                    if success:
                        update_data = {
                            'excelUpdated': True,
                            'processedAt': firestore.SERVER_TIMESTAMP,
                            'processedBy': 'secure_worker_v4',
                            'securityLevel': 'enhanced'
                        }
                        
                        # 기존 에러 정보 정리
                        if 'processingError' in cert_data:
                            update_data['processingError'] = firestore.DELETE_FIELD
                        if 'readyForExcel' in cert_data:
                            update_data['readyForExcel'] = False
                        
                        cert_ref.update(update_data)
                        return "✅ 성공"
                        
                    else:
                        cert_ref.update({
                            'excelSaveError': 'SECURE_SAVE_ERROR',
                            'excelSaveErrorAt': firestore.SERVER_TIMESTAMP,
                            'retryCount': firestore.Increment(1)
                        })
                        return "⚠️ 재시도설정"
                        
                except Exception:
                    return "❌ 업데이트실패"
            
            # 🔒 제한된 병렬 처리 (최대 2개 스레드)
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_cert = {
                    executor.submit(update_single_flag_secure, cert): cert 
                    for cert in processed_certs
                }
                
                results = []
                for future in as_completed(future_to_cert, timeout=60):
                    try:
                        result = future.result(timeout=10)
                        results.append(result)
                    except Exception:
                        results.append("❌ 타임아웃")
            
            success_count = sum(1 for r in results if r.startswith('✅'))
            logger.info(f"✅ 보안 플래그 업데이트: {success_count}/{len(results)}")
            
        except Exception as e:
            logger.error(f"❌ 보안 배치 플래그 업데이트 실패")

# ===================================================================
# 🔒 보안 강화된 배치 처리
# ===================================================================
def secure_process_batch():
    """보안 강화된 배치 처리"""
    operation_id = f"secure_batch_{int(time.time())}"
    
    with manage_operation(operation_id):
        try:
            batch_start_time = datetime.now(timezone.utc)
            
            # 🔒 처리할 수료증 보안 조회
            pending_certs = secure_collection_group_query(limit=BATCH_SIZE)
            
            if not pending_certs:
                logger.debug("😴 처리할 보안 수료증 없음")
                return
            
            logger.info(f"🔒 {len(pending_certs)}개 수료증 보안 배치 처리 시작")
            
            # 마스터 엑셀 보안 로드
            df = secure_load_master_excel()
            original_row_count = len(df)
            
            # 처리 통계
            success_count = 0
            error_count = 0
            processed_certs = []
            
            # 각 수료증 보안 처리
            for i, (user_uid, cert_id, cert_data) in enumerate(pending_certs, 1):
                if shutdown_flag:
                    logger.info("🛑 종료 플래그 감지, 보안 배치 중단")
                    break
                
                # 🔒 개별 처리 타임아웃 (30초)
                try:
                    import signal
                    
                    def timeout_handler(signum, frame):
                        raise TimeoutError("개별 처리 타임아웃")
                    
                    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(30)
                    
                    try:
                        success, df = secure_process_certificate(user_uid, cert_id, cert_data, df)
                        
                        if success:
                            success_count += 1
                            processed_certs.append((user_uid, cert_id, cert_data))
                        else:
                            error_count += 1
                            
                    finally:
                        signal.alarm(0)
                        signal.signal(signal.SIGALRM, old_handler)
                        
                except TimeoutError:
                    logger.warning(f"⚠️ 개별 처리 타임아웃")
                    error_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ 개별 처리 오류")
                    error_count += 1
            
            # 🔒 Excel 보안 저장 및 플래그 업데이트
            if success_count > 0:
                new_row_count = len(df)
                logger.info(f"📊 보안 Excel 저장: {original_row_count}행 → {new_row_count}행")
                
                excel_save_success = secure_save_master_excel(df)
                secure_update_certificate_flags_batch(processed_certs, success=excel_save_success)
                
                processing_time = (datetime.now(timezone.utc) - batch_start_time).total_seconds()
                
                if excel_save_success:
                    logger.info(f"🎉 보안 배치 완료 - ✅성공: {success_count}, ❌실패: {error_count}, ⏱️시간: {processing_time:.1f}초")
                else:
                    logger.error(f"❌ 보안 Excel 저장 실패 - 재시도 예정")
            else:
                logger.info(f"📊 보안 배치 완료 - 처리된 항목 없음 (❌실패: {error_count})")
            
        except Exception as e:
            logger.error(f"❌ 보안 배치 처리 중 오류")

# ===================================================================
# 🔒 헬스체크 및 모니터링 (보안 강화)
# ===================================================================
def secure_update_health_status():
    """보안 강화된 헬스체크"""
    try:
        # 🔒 보안 헬스 파일 (랜덤 경로)
        health_token = secrets.token_hex(8)
        health_file = Path(f'/tmp/secure_worker_{health_token}.health')
        
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'active_operations': len(current_operations),
            'security_level': 'enhanced',
            'worker_version': 'secure_v4.0'
        }
        
        with open(health_file, 'w') as f:
            json.dump(health_data, f)
        
        # 이전 헬스 파일들 정리
        for old_file in Path('/tmp').glob('secure_worker_*.health'):
            try:
                if old_file != health_file and (time.time() - old_file.stat().st_mtime) > 300:
                    old_file.unlink()
            except Exception:
                pass
                
    except Exception:
        pass  # 헬스체크 실패는 로그 남기지 않음

def secure_get_statistics() -> Dict[str, Any]:
    """보안 강화된 통계 정보"""
    try:
        # 🔒 30초 캐싱
        current_time = time.time()
        if hasattr(secure_get_statistics, 'cache_time') and (current_time - secure_get_statistics.cache_time) < 30:
            return secure_get_statistics.cached_stats
        
        # 샘플링으로 성능 향상 (보안 제한)
        sample_size = min(50, BATCH_SIZE * 2)
        
        try:
            pending_query = db.collection_group('completedCertificates') \
                             .where('excelUpdated', '==', False) \
                             .where('sentToAdmin', '==', True) \
                             .limit(sample_size)
            pending_sample = list(pending_query.stream())
            
            processed_query = db.collection_group('completedCertificates') \
                               .where('excelUpdated', '==', True) \
                               .limit(sample_size)
            processed_sample = list(processed_query.stream())
            
            stats = {
                'pending_sample': len(pending_sample),
                'processed_sample': len(processed_sample),
                'is_sample': True,
                'sample_size': sample_size,
                'security_level': 'enhanced'
            }
            
        except Exception:
            stats = {
                'pending_sample': -1,
                'processed_sample': -1,
                'error': 'query_failed',
                'security_level': 'enhanced'
            }
        
        # 캐시 저장
        secure_get_statistics.cached_stats = stats
        secure_get_statistics.cache_time = current_time
        
        return stats
        
    except Exception:
        return {'error': 'stats_failed', 'security_level': 'enhanced'}

# ===================================================================
# 🔒 보안 강화된 메인 워커 루프
# ===================================================================
def run_secure_worker():
    """보안 강화된 메인 워커"""
    logger.info(f"🔒 Secure Certificate Worker v4.0 시작")
    logger.info(f"🛡️ 보안 레벨: Enhanced")
    logger.info(f"⏱️ 폴링 간격: {POLL_INTERVAL_SECONDS}초")
    logger.info(f"📦 배치 크기: {BATCH_SIZE}")
    logger.info(f"🔄 최대 재시도: {MAX_RETRY_COUNT}")
    logger.info(f"🚦 최대 동시 작업: {MAX_CONCURRENT_OPERATIONS}")
    
    # 초기 보안 설정
    secure_update_health_status()
    
    # 초기 통계
    initial_stats = secure_get_statistics()
    logger.info(f"📊 초기 보안 통계 - 대기 샘플: {initial_stats.get('pending_sample', 'N/A')}")
    
    iteration = 0
    last_activity_time = None
    consecutive_empty_batches = 0
    
    while not shutdown_flag:
        try:
            iteration += 1
            
            # 🔒 보안 헬스체크 업데이트
            if iteration % 5 == 0:  # 5회마다 실행
                secure_update_health_status()
            
            # 🔒 동시 작업 수 보안 체크
            if len(current_operations) > MAX_CONCURRENT_OPERATIONS:
                logger.warning(f"🚨 보안 제한: 동시 작업 수 초과 ({len(current_operations)}>{MAX_CONCURRENT_OPERATIONS})")
                time.sleep(5)
                continue
            
            # 배치 처리 실행
            batch_start_time = datetime.now(timezone.utc)
            
            # 이전 통계 저장
            prev_stats = secure_get_statistics()
            
            # 🔒 보안 배치 처리
            secure_process_batch()
            
            # 처리 후 통계 확인
            current_stats = secure_get_statistics()
            
            # 활동 감지
            if current_stats.get('pending_sample', 0) != prev_stats.get('pending_sample', 0):
                last_activity_time = batch_start_time
                consecutive_empty_batches = 0
            else:
                consecutive_empty_batches += 1
            
            # 🔒 보안 상태 로깅 (적응적 주기)
            log_interval = 5 if consecutive_empty_batches > 3 else 10
            if iteration % log_interval == 0:
                logger.info(f"🔒 보안 상태 - 반복: {iteration}, 활성작업: {len(current_operations)}")
                
                if last_activity_time:
                    idle_time = (datetime.now(timezone.utc) - last_activity_time).total_seconds()
                    logger.info(f"🕐 마지막 활동: {idle_time:.0f}초 전")
            
            # 🔒 보안 동적 대기 시간
            if consecutive_empty_batches > 5:
                sleep_time = min(POLL_INTERVAL_SECONDS * 2, 180)  # 최대 3분
            else:
                sleep_time = POLL_INTERVAL_SECONDS
            
            # 인터럽트 가능한 보안 대기
            for _ in range(sleep_time):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ 키보드 인터럽트 - 보안 종료")
            break
        except Exception as e:
            logger.error(f"❌ 보안 워커 루프 오류")
            error_sleep = min(POLL_INTERVAL_SECONDS, 60)
            logger.info(f"🔄 {error_sleep}초 후 보안 재시작...")
            time.sleep(error_sleep)
    
    # 🔒 보안 종료 정리
    with operations_lock:
        if current_operations:
            logger.info(f"🔄 {len(current_operations)}개 보안 작업 완료 대기...")
            for _ in range(60):  # 최대 60초 대기
                if not current_operations:
                    break
                time.sleep(1)
    
    logger.info("👋 Secure Certificate Worker 안전 종료 완료")

# ===================================================================
# 🔒 보안 강화된 엔트리 포인트
# ===================================================================
if __name__ == "__main__":
    try:
        # 🔒 최종 보안 검증
        logger.info("🔍 최종 보안 검증 중...")
        
        # Firebase 연결 보안 재확인
        test_token = SecurityValidator.generate_secure_token()
        test_collection = db.collection('_secure_worker_health')
        test_doc_id = f"secure_test_{test_token[:8]}"
        
        test_doc = test_collection.document(test_doc_id)
        test_doc.set({
            'timestamp': firestore.SERVER_TIMESTAMP,
            'worker': 'secure_certificate_worker_v4',
            'security_level': 'enhanced',
            'startup_time': datetime.now(timezone.utc).isoformat(),
            'test_token': test_token[:16]
        })
        
        # 테스트 문서 즉시 삭제
        test_doc.delete()
        
        logger.info("✅ 최종 보안 검증 완료")
        
        # 🔒 보안 워커 시작
        run_secure_worker()
        
    except Exception as e:
        logger.error(f"❌ 보안 워커 시작 실패")
        sys.exit(1)