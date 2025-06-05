# worker/firestore_poller.py

import os
import time
import io
import sys
import pandas as pd
from datetime import datetime
from google.cloud import firestore, storage as gcs_storage
from google.oauth2 import service_account

# ─────────────────────────────────────────────────────────────────────────────
# 1) Firebase/GCS 자격증명 초기화
# ─────────────────────────────────────────────────────────────────────────────
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
    print("[INIT] Initialized Firestore and GCS clients with service account.", file=sys.stderr)
else:
    # 로컬 개발 환경: GOOGLE_APPLICATION_CREDENTIALS를 사용하거나
    # 사용자 머신에 설정된 ADC(Application Default Credentials) 사용
    db = firestore.Client()
    gcs = gcs_storage.Client()
    print("[INIT] Initialized Firestore and GCS clients with default credentials.", file=sys.stderr)

# ─────────────────────────────────────────────────────────────────────────────
# 2) GCS 버킷 이름 (워커 전용)
# ─────────────────────────────────────────────────────────────────────────────
BUCKET_NAME     = os.getenv("GCLOUD_STORAGE_BUCKET", f"{os.getenv('project_id')}.appspot.com")
MASTER_FILENAME = "master_certificates.xlsx"
bucket          = gcs.bucket(BUCKET_NAME)
print(f"[INIT] Using GCS bucket: {BUCKET_NAME}", file=sys.stderr)

# ─────────────────────────────────────────────────────────────────────────────
# 3) 아직 엑셀에 업데이트되지 않은 수료증 문서(fetch_unprocessed_certs)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_unprocessed_certs():
    results = []
    try:
        users = db.collection("users").stream()
    except Exception as e:
        print(f"[ERROR] Failed to fetch users collection: {e}", file=sys.stderr)
        return results

    for user_doc in users:
        user_uid = user_doc.id
        certs_ref = user_doc.reference.collection("completedCertificates")
        try:
            # excelUpdated == False인 문서만 가져오기
            query = certs_ref.where("excelUpdated", "==", False)
            docs = list(query.stream())
        except Exception as e:
            print(f"[ERROR] Failed to query completedCertificates for user {user_uid}: {e}", file=sys.stderr)
            continue

        for cert_doc in docs:
            results.append((user_uid, cert_doc.id, cert_doc.to_dict()))

    return results

# ─────────────────────────────────────────────────────────────────────────────
# 4) 수료증 정보를 엑셀에 추가/갱신하는 함수(update_excel_for_cert)
# ─────────────────────────────────────────────────────────────────────────────
def update_excel_for_cert(user_uid, cert_id, cert_info):
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
            print(f"[WARN] User document {user_uid} does not exist.", file=sys.stderr)
    except Exception as e:
        user_name  = ""
        user_phone = ""
        user_email = ""
        print(f"[ERROR] Failed to read user document {user_uid}: {e}", file=sys.stderr)

    # --- 2) 수료증 정보에서 강의 제목, 발급 시각, PDF URL 읽어오기 ---
    lecture_title = cert_info.get("lectureTitle", cert_id)
    issued_at_ts   = cert_info.get("issuedAt")
    pdf_url        = cert_info.get("pdfUrl", "")

    # Firestore Timestamp → 문자열
    if hasattr(issued_at_ts, "to_datetime"):
        issued_str = issued_at_ts.to_datetime().strftime("%Y-%m-%d %H:%M:%S")
    else:
        issued_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # --- 3) 워커가 실제로 엑셀에 행을 추가한 시각(업데이트 날짜) ---
    updated_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    master_blob = bucket.blob(MASTER_FILENAME)

    # --- 4) 기존 엑셀 다운로드 시도 ---
    try:
        existing_bytes = master_blob.download_as_bytes()
        excel_buffer   = io.BytesIO(existing_bytes)
        df             = pd.read_excel(excel_buffer, engine="openpyxl")
        print(f"[DEBUG] Downloaded existing {MASTER_FILENAME} ({len(df)} rows).", file=sys.stderr)
    except Exception as e:
        # 파일이 없거나 읽기 실패 시: 빈 DataFrame 생성
        print(f"[DEBUG] Could not download master Excel, creating new DataFrame: {e}", file=sys.stderr)
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

    # --- 5) 새 행(row) 생성 및 추가 ---
    new_row = {
        '업데이트 날짜': updated_date,
        '사용자 UID':    user_uid,
        '전화번호':      user_phone,
        '이메일':        user_email,
        '사용자 이름':   user_name,
        '강의 제목':     lecture_title,
        '발급 일시':     issued_str,
        'PDF URL':       pdf_url
    }
    df = df.append(new_row, ignore_index=True)
    print(f"[DEBUG] Appended new row for user {user_uid}, cert {cert_id}. Total rows now: {len(df)}", file=sys.stderr)

    # --- 6) DataFrame → 엑셀(BytesIO)로 쓰기 ---
    out_buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(out_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Certificates")
        out_buffer.seek(0)
        print(f"[DEBUG] Successfully wrote DataFrame to Excel buffer.", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Failed to write DataFrame to Excel buffer: {e}", file=sys.stderr)
        return

    # --- 7) GCS에 덮어쓰기 업로드 ---
    try:
        master_blob.upload_from_file(
            out_buffer,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print(f"[EXCEL UPDATE] Added cert {cert_id} for user {user_uid} at {updated_date}", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Failed to upload updated Excel to GCS: {e}", file=sys.stderr)
        return

    # --- 8) Firestore 문서에 excelUpdated=True로 표시 ---
    try:
        cert_ref = db.collection("users").document(user_uid) \
                     .collection("completedCertificates").document(cert_id)
        cert_ref.update({"excelUpdated": True})
        print(f"[DEBUG] Set excelUpdated=True for {user_uid}/{cert_id}.", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Failed to update excelUpdated flag for {user_uid}/{cert_id}: {e}", file=sys.stderr)

# ─────────────────────────────────────────────────────────────────────────────
# 5) 메인 루프: 주기적으로 폴링 실행(run_poller)
# ─────────────────────────────────────────────────────────────────────────────
def run_poller(interval_seconds=60):
    print(f"[POLL START] Polling Firestore every {interval_seconds} seconds.", file=sys.stderr)
    while True:
        try:
            unprocessed = fetch_unprocessed_certs()
            print(f"[POLL] Found {len(unprocessed)} certificates to process.", file=sys.stderr)
            for user_uid, cert_id, cert_info in unprocessed:
                try:
                    update_excel_for_cert(user_uid, cert_id, cert_info)
                except Exception as e:
                    print(f"[ERROR] Fail to update Excel for {user_uid}/{cert_id}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Polling loop error: {e}", file=sys.stderr)
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_poller(interval_seconds=60)
