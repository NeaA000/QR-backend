# worker/firestore_poller.py

import os
import time
import io
import pandas as pd
from datetime import datetime
from google.cloud import firestore, storage as gcs_storage
from google.oauth2 import service_account

# ─────────────────────────────────────────────────────────────────────────────
# 1) Firebase/GCS 자격증명 초기화 (기존 환경변수명 재사용)
# ─────────────────────────────────────────────────────────────────────────────

# Railsway(혹은 다른 호스팅)에서 아래 환경변수들이 이미 등록되어 있다고 가정합니다:
#   - project_id
#   - client_email
#   - private_key
#   - private_key_id
#   - client_id
#   - client_x509_cert_url
#   - auth_uri
#   - token_uri
#   - auth_provider_x509_cert_url
#   - GCLOUD_STORAGE_BUCKET

if os.getenv("private_key") and os.getenv("client_email"):
    creds = service_account.Credentials.from_service_account_info({
        "type":                        "service_account",
        "project_id":                  os.getenv("project_id"),
        "private_key_id":              os.getenv("private_key_id", ""),
        # 기존 private_key 값을 줄바꿈(\n) 포함 형태로 저장했다고 가정
        "private_key":                 os.getenv("private_key").replace("\\n", "\n"),
        "client_email":                os.getenv("client_email"),
        "client_id":                   os.getenv("client_id", ""),
        # 나머지 URI 계열은 고정값 혹은 이미 등록된 값 그대로 사용
        "auth_uri":                    os.getenv("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
        "token_uri":                   os.getenv("token_uri", "https://oauth2.googleapis.com/token"),
        "auth_provider_x509_cert_url": os.getenv("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url":        os.getenv("client_x509_cert_url", "")
    })
    db = firestore.Client(credentials=creds, project=os.getenv("project_id"))
    gcs = gcs_storage.Client(credentials=creds, project=os.getenv("project_id"))
else:
    # 로컬 개발 환경에서는 GOOGLE_APPLICATION_CREDENTIALS를 사용하거나,
    # 직접 JSON 파일을 참조하도록 설정할 수 있습니다.
    db = firestore.Client()
    gcs = gcs_storage.Client()

# ─────────────────────────────────────────────────────────────────────────────
# 2) GCS 버킷 이름 (워커 전용)
# ─────────────────────────────────────────────────────────────────────────────
# Railway Settings → Variables 에서 아래 키 하나만 추가하면 됩니다:
#   GCLOUD_STORAGE_BUCKET = qrjbsafetyeducation.appspot.com
BUCKET_NAME     = os.getenv("GCLOUD_STORAGE_BUCKET", f"{os.getenv('project_id')}.appspot.com")
MASTER_FILENAME = "master_certificates.xlsx"

bucket = gcs.bucket(BUCKET_NAME)

# ─────────────────────────────────────────────────────────────────────────────
# 3) 아직 엑셀에 업데이트되지 않은 수료증 문서 가져오기
# ─────────────────────────────────────────────────────────────────────────────
def fetch_unprocessed_certs():
    results = []
    # users 컬렉션 밑의 모든 문서를 순회
    for user_doc in db.collection("users").stream():
        user_uid = user_doc.id
        certs_ref = user_doc.reference.collection("completedCertificates")
        # excelUpdated == False인 문서만 가져온다.
        query = certs_ref.where("excelUpdated", "==", False)
        for cert_doc in query.stream():
            results.append((user_uid, cert_doc.id, cert_doc.to_dict()))
    return results

# ─────────────────────────────────────────────────────────────────────────────
# 4) 단일 수료증을 엑셀에 추가/덮어쓰기
# ─────────────────────────────────────────────────────────────────────────────
def update_excel_for_cert(user_uid, cert_id, cert_info):
    lecture_title = cert_info.get("lectureTitle", cert_id)
    issued_at_ts   = cert_info.get("issuedAt")
    pdf_url        = cert_info.get("pdfUrl", "")

    # Firestore Timestamp → 문자열
    if hasattr(issued_at_ts, "to_datetime"):
        issued_str = issued_at_ts.to_datetime().strftime("%Y-%m-%d %H:%M:%S")
    else:
        issued_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    master_blob = bucket.blob(MASTER_FILENAME)

    try:
        # ① 기존 엑셀 다운로드
        existing_bytes = master_blob.download_as_bytes()
        excel_buffer   = io.BytesIO(existing_bytes)
        df             = pd.read_excel(excel_buffer, engine="openpyxl")
    except Exception:
        # 파일이 없거나 읽기 오류인 경우, 헤더만 있는 빈 DataFrame 생성
        df = pd.DataFrame(columns=["User UID", "Lecture Title", "Issued At", "PDF URL"])

    # ② 새로운 행(row) 추가
    new_row = {
        "User UID":      user_uid,
        "Lecture Title": lecture_title,
        "Issued At":     issued_str,
        "PDF URL":       pdf_url
    }
    df = df.append(new_row, ignore_index=True)

    # ③ DataFrame → 엑셀 BytesIO로 쓰기
    out_buffer = io.BytesIO()
    with pd.ExcelWriter(out_buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Certificates")
    out_buffer.seek(0)

    # ④ GCS에 덮어쓰기 업로드
    master_blob.upload_from_file(
        out_buffer,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    print(f"[EXCEL UPDATE] Added cert {cert_id} for user {user_uid}")

    # ⑤ Firestore 문서에 excelUpdated=True 표시
    cert_ref = db.collection("users").document(user_uid) \
                 .collection("completedCertificates").document(cert_id)
    cert_ref.update({"excelUpdated": True})

# ─────────────────────────────────────────────────────────────────────────────
# 5) 메인 루프: 주기적으로 폴링 실행
# ─────────────────────────────────────────────────────────────────────────────
def run_poller(interval_seconds=60):
    print(f"[POLL START] Polling Firestore every {interval_seconds} seconds.")
    while True:
        try:
            unprocessed = fetch_unprocessed_certs()
            if unprocessed:
                print(f"[POLL] {len(unprocessed)} certificates to process.")
            for user_uid, cert_id, cert_info in unprocessed:
                try:
                    update_excel_for_cert(user_uid, cert_id, cert_info)
                except Exception as e:
                    print(f"[ERROR] Fail to update Excel for {user_uid}/{cert_id}: {e}")
        except Exception as e:
            print(f"[ERROR] Polling loop error: {e}")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_poller(interval_seconds=60)
