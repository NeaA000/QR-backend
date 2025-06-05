# worker/firestore_poller.py

import os
import time
import io
import warnings
import pandas as pd
from datetime import datetime, timezone
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
    print("[INIT] Initialized Firestore and GCS clients with service account.")
else:
    # 로컬 개발 환경: GOOGLE_APPLICATION_CREDENTIALS를 사용하거나
    # 사용자 머신에 설정된 ADC(Application Default Credentials) 사용
    db = firestore.Client()
    gcs = gcs_storage.Client()
    print("[INIT] Initialized Firestore and GCS clients with default credentials.")

# ─────────────────────────────────────────────────────────────────────────────
# 2) GCS 버킷 이름 (워커 전용)
# ─────────────────────────────────────────────────────────────────────────────
# 환경 변수 GCLOUD_STORAGE_BUCKET 에서 가져오며, 없으면 <project_id>.appspot.com 으로 대체
BUCKET_NAME     = os.getenv("GCLOUD_STORAGE_BUCKET", f"{os.getenv('project_id')}.appspot.com")
MASTER_FILENAME = "master_certificates.xlsx"
bucket          = gcs.bucket(BUCKET_NAME)
print(f"[INIT] Using GCS bucket: {BUCKET_NAME}")

# ─────────────────────────────────────────────────────────────────────────────
# 3) 아직 엑셀에 업데이트되지 않은 수료증 문서(fetch_unprocessed_certs)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_unprocessed_certs():
    """
    collection_group을 사용해서 users/{uid}/completedCertificates 아래의 모든 문서를 조회.
    excelUpdated가 False인 문서만 결과 리스트에 추가.
    반환 값: [(user_uid, cert_id, cert_data_dict), ...]
    """
    results = []
    try:
        # 모든 completedCertificates 문서를 가져옴
        all_certs = list(db.collection_group("completedCertificates").stream())
    except Exception as e:
        print(f"[ERROR] Failed to perform collection_group query: {e}")
        return results

    for cert_doc in all_certs:
        try:
            data = cert_doc.to_dict()
            # excelUpdated 필드가 True라면 이미 처리된 문서이므로 건너뜀
            if data.get("excelUpdated", False):
                continue

            # 문서 경로 예시: "users/{userId}/completedCertificates/{certId}"
            path_parts = cert_doc.reference.path.split("/")
            user_uid = path_parts[1]
            cert_id  = path_parts[3]

            results.append((user_uid, cert_id, data))
        except Exception as e:
            print(f"[ERROR] Failed to parse or filter document {cert_doc.id}: {e}")

    return results

# ─────────────────────────────────────────────────────────────────────────────
# 4) 수료증 정보를 엑셀에 추가/갱신하는 함수(update_excel_for_cert)
# ─────────────────────────────────────────────────────────────────────────────
def update_excel_for_cert(user_uid, cert_id, cert_info):
    """
    1) Firestore에서 users/{user_uid} 문서를 읽어 사용자 이름, 전화번호, 이메일 획득
    2) cert_info에서 lectureTitle, issuedAt, pdfUrl 읽어오기
    3) GCS에 master_certificates.xlsx 다운로드 (없으면 새 DataFrame 생성)
    4) Pandas DataFrame에서 불필요한 열('User UID','Lecture Title','Issued At') 삭제
    5) 새 행 추가 (concat 사용)
    6) 수정된 DataFrame을 엑셀 파일로 쓰고 GCS에 덮어쓰기 업로드
    7) Firestore completedCertificates/{cert_id}.update({"excelUpdated": True})
    """
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
            print(f"[WARN] User document {user_uid} does not exist.")
    except Exception as e:
        user_name  = ""
        user_phone = ""
        user_email = ""
        print(f"[ERROR] Failed to read user document {user_uid}: {e}")

    # --- 2) 수료증 정보에서 강의 제목, 발급 시각, PDF URL 읽어오기 ---
    lecture_title = cert_info.get("lectureTitle", cert_id)
    issued_at_ts   = cert_info.get("issuedAt")
    pdf_url        = cert_info.get("pdfUrl", "")

    # Firestore Timestamp → 문자열
    if hasattr(issued_at_ts, "to_datetime"):
        issued_str = issued_at_ts.to_datetime().strftime("%Y-%m-%d %H:%M:%S")
    else:
        # DeprecationWarning 대응: timezone-aware 객체를 사용
        issued_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # --- 3) 워커가 실제로 엑셀에 행을 추가한 시각(업데이트 날짜) ---
    updated_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    master_blob = bucket.blob(MASTER_FILENAME)

    # --- 4) 기존 엑셀 다운로드 시도 ---
    try:
        existing_bytes = master_blob.download_as_bytes()
        excel_buffer   = io.BytesIO(existing_bytes)
        df             = pd.read_excel(excel_buffer, engine="openpyxl")
        print(f"[DEBUG] Downloaded existing {MASTER_FILENAME} ({len(df)} rows).")
    except Exception as e:
        # 파일이 없거나 읽기 실패 시: 빈 DataFrame 생성
        print(f"[DEBUG] Could not download master Excel, creating new DataFrame: {e}")
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

    # --- 4.5) 기존 DataFrame에서 불필요한 열 삭제 ---
    # 이전 버전에서 남아 있을 수 있는 영어 컬럼(header)들을 제거합니다.
    for col in ['User UID', 'Lecture Title', 'Issued At']:
        if col in df.columns:
            df = df.drop(columns=[col])
    # 만약 'PDF URL' 이 두 번 중복되어 있다면, 필요에 따라 하나만 남길 수도 있습니다.
    # 예시: if 'PDF URL' 컬럼이 여러 번 있다면 첫 번째를 제외하고 제거하려면:
    # if df.columns.duplicated().any():
    #     df = df.loc[:, ~df.columns.duplicated()]

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

    # Pandas 2.x에서 append()가 사라졌으므로 concat 사용
    new_df = pd.DataFrame([new_row])
    df = pd.concat([df, new_df], ignore_index=True)

    print(f"[DEBUG] Appended new row for user {user_uid}, cert {cert_id}. Total rows now: {len(df)}")

    # --- 6) DataFrame → 엑셀(BytesIO)로 쓰기 ---
    out_buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(out_buffer, engine="openpyxl") as writer:
            # index=False: 인덱스 열을 빼고
            # header=True (기본값이므로 생략): 컬럼명(한글 컬럼)만 남깁니다
            df.to_excel(writer, index=False, sheet_name="Certificates")
        out_buffer.seek(0)
        print(f"[DEBUG] Successfully wrote DataFrame to Excel buffer.")
    except Exception as e:
        print(f"[ERROR] Failed to write DataFrame to Excel buffer: {e}")
        return

    # --- 7) GCS에 덮어쓰기 업로드 ---
    try:
        master_blob.upload_from_file(
            out_buffer,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print(f"[EXCEL UPDATE] Added cert {cert_id} for user {user_uid} at {updated_date}")
    except Exception as e:
        print(f"[ERROR] Failed to upload updated Excel to GCS: {e}")
        return

    # --- 8) Firestore 문서에 excelUpdated=True로 표시 ---
    try:
        cert_ref = db.collection("users").document(user_uid) \
                     .collection("completedCertificates").document(cert_id)
        cert_ref.update({"excelUpdated": True})
        print(f"[DEBUG] Set excelUpdated=True for {user_uid}/{cert_id}.")
    except Exception as e:
        print(f"[ERROR] Failed to update excelUpdated flag for {user_uid}/{cert_id}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 5) 메인 루프: 주기적으로 폴링 실행(run_poller)
# ─────────────────────────────────────────────────────────────────────────────
def run_poller(interval_seconds=60):
    print(f"[POLL START] Polling Firestore every {interval_seconds} seconds.")
    while True:
        try:
            unprocessed = fetch_unprocessed_certs()
            print(f"[POLL] Found {len(unprocessed)} certificates to process.")
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
