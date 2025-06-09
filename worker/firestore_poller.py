# worker/firestore_poller.py

import os
import time
import io
import warnings
import pandas as pd
from datetime import datetime, timezone
from google.cloud import firestore, storage as gcs_storage
from google.oauth2 import service_account
import signal
import sys
import logging
from typing import List, Tuple, Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정 개선
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('worker.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1) Firebase/GCS 자격증명 초기화
# ─────────────────────────────────────────────────────────────────────────────
def initialize_clients():
    """Firebase 및 GCS 클라이언트 초기화"""
    try:
        if os.getenv("private_key") and os.getenv("client_email"):
            # Railway 환경변수를 그대로 사용해서 서비스 계정 자격증명 생성
            creds = service_account.Credentials.from_service_account_info({
                "type":                        "service_account",
                "project_id":                  os.getenv("project_id"),
                "private_key_id":              os.getenv("private_key_id", ""),
                "private_key":                 os.getenv("private_key").replace("\\n", "\n"),
                "client_email":                os.getenv("client_email"),
                "client_id":                   os.getenv("client_id", ""),
                "auth_uri":                    os.getenv("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
                "token_uri":                   os.getenv("token_uri", "https://oauth2.googleapis.com/token"),
                "auth_provider_x509_cert_url": os.getenv("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
                "client_x509_cert_url":        os.getenv("client_x509_cert_url", "")
            })
            db = firestore.Client(credentials=creds, project=os.getenv("project_id"))
            gcs = gcs_storage.Client(credentials=creds, project=os.getenv("project_id"))
            logger.info("🔑 Initialized Firestore and GCS clients with service account.")
        else:
            # 로컬 개발 환경: GOOGLE_APPLICATION_CREDENTIALS를 사용하거나
            # 사용자 머신에 설정된 ADC(Application Default Credentials) 사용
            db = firestore.Client()
            gcs = gcs_storage.Client()
            logger.info("🔑 Initialized Firestore and GCS clients with default credentials.")
        
        return db, gcs
    except Exception as e:
        logger.error(f"❌ Failed to initialize clients: {e}")
        raise

db, gcs = initialize_clients()

# ─────────────────────────────────────────────────────────────────────────────
# 2) GCS 버킷 이름 (워커 전용)
# ─────────────────────────────────────────────────────────────────────────────
BUCKET_NAME     = os.getenv("GCLOUD_STORAGE_BUCKET", f"{os.getenv('project_id')}.appspot.com")
MASTER_FILENAME = "master_certificates.xlsx"
bucket          = gcs.bucket(BUCKET_NAME)
logger.info(f"📦 Using GCS bucket: {BUCKET_NAME}")

# ─────────────────────────────────────────────────────────────────────────────
# 3) 전역 변수: 정상 종료 시그널 처리
# ─────────────────────────────────────────────────────────────────────────────
shutdown_flag = False

def signal_handler(signum, frame):
    """SIGINT/SIGTERM 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 Received signal {signum}. Initiating graceful shutdown...")
    shutdown_flag = True

# 시그널 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ─────────────────────────────────────────────────────────────────────────────
# 4) 아직 엑셀에 업데이트되지 않은 수료증 문서(fetch_unprocessed_certs)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_unprocessed_certs() -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    collection_group을 사용해서 users/{uid}/completedCertificates 아래의 모든 문서를 조회.
    excelUpdated가 False이고 readyForExcel이 True인 문서만 결과 리스트에 추가.
    반환 값: [(user_uid, cert_id, cert_data_dict), ...]
    """
    results = []
    try:
        # readyForExcel=True이고 excelUpdated=False인 문서만 조회 (더 효율적)
        query = db.collection_group("completedCertificates") \
                  .where("readyForExcel", "==", True) \
                  .where("excelUpdated", "==", False) \
                  .limit(100)  # 한 번에 처리할 최대 개수 제한
        
        all_certs = list(query.stream())
        logger.debug(f"🔍 Query returned {len(all_certs)} documents")
        
    except Exception as e:
        logger.error(f"❌ Failed to perform collection_group query: {e}")
        return results

    for cert_doc in all_certs:
        try:
            data = cert_doc.to_dict()
            
            # 추가 검증: PDF URL이 있는지 확인
            if not data.get("pdfUrl"):
                logger.warning(f"⚠️ Document {cert_doc.id} has no pdfUrl. Skipping.")
                continue

            # 문서 경로 예시: "users/{userId}/completedCertificates/{certId}"
            path_parts = cert_doc.reference.path.split("/")
            if len(path_parts) < 4:
                logger.warning(f"⚠️ Invalid document path: {cert_doc.reference.path}")
                continue
                
            user_uid = path_parts[1]
            cert_id  = path_parts[3]

            results.append((user_uid, cert_id, data))
            
        except Exception as e:
            logger.error(f"❌ Failed to parse document {cert_doc.id}: {e}")

    return results

# ─────────────────────────────────────────────────────────────────────────────
# 5) 수료증 정보를 엑셀에 추가/갱신하는 함수(update_excel_for_cert)
# ─────────────────────────────────────────────────────────────────────────────
def update_excel_for_cert(user_uid: str, cert_id: str, cert_info: Dict[str, Any]) -> bool:
    """
    수료증 정보를 마스터 엑셀에 추가하고 Firestore 플래그를 업데이트
    
    Returns:
        bool: 성공 시 True, 실패 시 False
    """
    try:
        # --- 1) 사용자 프로필(이름, 전화번호, 이메일) 읽어오기 ---
        user_ref = db.collection("users").document(user_uid)
        try:
            user_snapshot = user_ref.get()
            if user_snapshot.exists:
                user_data  = user_snapshot.to_dict()
                user_name  = user_data.get("name", "")
                user_phone = user_data.get("phone", "")
                user_email = user_data.get("email", "")
            else:
                user_name  = ""
                user_phone = ""
                user_email = ""
                logger.warning(f"⚠️ User document {user_uid} does not exist.")
        except Exception as e:
            user_name  = ""
            user_phone = ""
            user_email = ""
            logger.error(f"❌ Failed to read user document {user_uid}: {e}")

        # --- 2) 수료증 정보에서 강의 제목, 발급 시각, PDF URL 읽어오기 ---
        lecture_title = cert_info.get("lectureTitle", cert_id)
        issued_at_ts   = cert_info.get("issuedAt")
        pdf_url        = cert_info.get("pdfUrl", "")

        # PDF URL 재검증
        if not pdf_url:
            logger.error(f"❌ No PDF URL found for cert {cert_id}")
            return False

        # Firestore Timestamp → 문자열
        if hasattr(issued_at_ts, "to_datetime"):
            issued_str = issued_at_ts.to_datetime().strftime("%Y-%m-%d %H:%M:%S")
        else:
            issued_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # --- 3) 워커가 실제로 엑셀에 행을 추가한 시각(업데이트 날짜) ---
        updated_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        master_blob = bucket.blob(MASTER_FILENAME)

        # --- 4) 기존 엑셀 다운로드 시도 ---
        try:
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer   = io.BytesIO(existing_bytes)
            df             = pd.read_excel(excel_buffer, engine="openpyxl")
            logger.debug(f"📥 Downloaded existing {MASTER_FILENAME} ({len(df)} rows).")
        except Exception as e:
            # 파일이 없거나 읽기 실패 시: 빈 DataFrame 생성
            logger.info(f"📄 Creating new master Excel file: {e}")
            df = pd.DataFrame(columns=[
                '업데이트 날짜',
                '사용자 UID',
                '전화번호',
                '이메일',
                '사용자 이름',
                '강의 제목',
                '발급 일시',
                'PDF URL',
                'Cert ID'
            ])

        # --- 4.5) 기존 DataFrame에서 불필요한 열 삭제 ---
        columns_to_remove = ['User UID', 'Lecture Title', 'Issued At']
        for col in columns_to_remove:
            if col in df.columns:
                df = df.drop(columns=[col])
                logger.debug(f"🗑️ Removed column: {col}")

        # --- 4.6) 'Cert ID' 컬럼 생성/존재 여부 확인 ---
        if 'Cert ID' not in df.columns:
            df['Cert ID'] = ""

        # --- 5) 중복 체크: 같은 cert_id가 이미 있는지 확인 ---
        if not df.empty and cert_id in df['Cert ID'].astype(str).values:
            logger.warning(f"⚠️ cert_id={cert_id} already exists in Excel. Marking as processed.")
            # excelUpdated 플래그만 True로 마킹하고 리턴
            _mark_as_processed(user_uid, cert_id)
            return True

        # --- 6) 새 행(row) 생성 및 추가 ---
        new_row = {
            '업데이트 날짜': updated_date,
            '사용자 UID':    user_uid,
            '전화번호':      user_phone,
            '이메일':        user_email,
            '사용자 이름':   user_name,
            '강의 제목':     lecture_title,
            '발급 일시':     issued_str,
            'PDF URL':       pdf_url,
            'Cert ID':       cert_id
        }

        new_df = pd.DataFrame([new_row])
        df = pd.concat([df, new_df], ignore_index=True)
        logger.debug(f"➕ Added new row for user {user_uid}, cert {cert_id}. Total rows: {len(df)}")

        # --- 7) DataFrame → 엑셀(BytesIO)로 쓰기 ---
        out_buffer = io.BytesIO()
        try:
            with pd.ExcelWriter(out_buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Certificates")
            out_buffer.seek(0)
            logger.debug(f"📊 Successfully wrote DataFrame to Excel buffer.")
        except Exception as e:
            logger.error(f"❌ Failed to write DataFrame to Excel buffer: {e}")
            return False

        # --- 8) GCS에 덮어쓰기 업로드 ---
        try:
            master_blob.upload_from_file(
                out_buffer,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            logger.info(f"✅ Added cert {cert_id} for user {user_uid} to master Excel")
        except Exception as e:
            logger.error(f"❌ Failed to upload updated Excel to GCS: {e}")
            return False

        # --- 9) Firestore 문서 플래그 업데이트 ---
        return _mark_as_processed(user_uid, cert_id)
        
    except Exception as e:
        logger.error(f"❌ Unexpected error updating Excel for {user_uid}/{cert_id}: {e}")
        return False

def _mark_as_processed(user_uid: str, cert_id: str) -> bool:
    """Firestore 문서에 처리 완료 플래그 설정"""
    try:
        cert_ref = db.collection("users").document(user_uid) \
                     .collection("completedCertificates").document(cert_id)
        cert_ref.update({
            "excelUpdated": True,
            "readyForExcel": False,
            "processedAt": firestore.SERVER_TIMESTAMP
        })
        logger.debug(f"✅ Marked {user_uid}/{cert_id} as processed")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to update flags for {user_uid}/{cert_id}: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# 6) 헬스체크 및 통계 함수들
# ─────────────────────────────────────────────────────────────────────────────
def get_pending_count() -> int:
    """처리 대기 중인 수료증 개수 조회"""
    try:
        query = db.collection_group("completedCertificates") \
                  .where("readyForExcel", "==", True) \
                  .where("excelUpdated", "==", False)
        return len(list(query.stream()))
    except Exception as e:
        logger.error(f"❌ Failed to get pending count: {e}")
        return -1

def log_statistics():
    """워커 통계 로깅"""
    try:
        pending_count = get_pending_count()
        if pending_count >= 0:
            logger.info(f"📊 Pending certificates: {pending_count}")
        else:
            logger.warning(f"⚠️ Could not retrieve pending count")
    except Exception as e:
        logger.error(f"❌ Error logging statistics: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 7) 메인 루프: 주기적으로 폴링 실행(run_poller)
# ─────────────────────────────────────────────────────────────────────────────
def run_poller(interval_seconds: int = 60):
    """
    메인 폴링 루프
    
    Args:
        interval_seconds: 폴링 간격 (초)
    """
    logger.info(f"🚀 Starting Firestore poller (interval: {interval_seconds}s)")
    
    # 시작 시 통계 로깅
    log_statistics()
    
    iteration_count = 0
    successful_updates = 0
    failed_updates = 0
    
    while not shutdown_flag:
        try:
            iteration_count += 1
            logger.debug(f"🔄 Polling iteration #{iteration_count}")
            
            # 처리할 수료증 조회
            unprocessed = fetch_unprocessed_certs()
            
            if unprocessed:
                logger.info(f"📋 Found {len(unprocessed)} certificates to process")
                
                for user_uid, cert_id, cert_info in unprocessed:
                    if shutdown_flag:
                        logger.info("🛑 Shutdown flag detected, stopping processing")
                        break
                        
                    try:
                        success = update_excel_for_cert(user_uid, cert_id, cert_info)
                        if success:
                            successful_updates += 1
                        else:
                            failed_updates += 1
                            
                    except Exception as e:
                        failed_updates += 1
                        logger.error(f"❌ Failed to update Excel for {user_uid}/{cert_id}: {e}")
            else:
                logger.debug("😴 No certificates to process")
            
            # 10번째 반복마다 통계 로깅
            if iteration_count % 10 == 0:
                logger.info(f"📈 Stats - Processed: {successful_updates}, Failed: {failed_updates}")
                log_statistics()
            
            # 종료 시그널 체크하면서 대기
            for _ in range(interval_seconds):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ Keyboard interrupt received")
            break
        except Exception as e:
            failed_updates += 1
            logger.error(f"❌ Polling loop error: {e}")
            time.sleep(min(interval_seconds, 30))  # 에러 시 최대 30초만 대기

    logger.info(f"🏁 Poller stopped. Final stats - Success: {successful_updates}, Failed: {failed_updates}")

if __name__ == "__main__":
    try:
        # 환경변수에서 폴링 간격 설정 (기본값: 60초)
        interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
        run_poller(interval_seconds=interval)
    except Exception as e:
        logger.error(f"❌ Worker startup failed: {e}")
        sys.exit(1)
    finally:
        logger.info("👋 Worker shutdown complete")