import os
import csv
import uuid
import re
from pathlib import Path
from datetime import datetime
from flask import Flask, request, render_template, jsonify
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image

# ✅ Firebase Admin SDK (환경변수 기반)
import firebase_admin
from firebase_admin import credentials, auth
import requests
import json

# ==== 사용자 설정 (환경변수로 관리) ====
AWS_ACCESS_KEY    = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY    = os.environ.get('AWS_SECRET_KEY')
REGION_NAME       = os.environ.get('REGION_NAME')
BUCKET_NAME       = os.environ.get('BUCKET_NAME')
APP_BASE_URL      = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
UPLOAD_LOG        = 'upload_log.csv'

# ==== Branch.io 설정 ====
BRANCH_KEY        = os.environ.get('BRANCH_KEY')
BRANCH_API_URL    = 'https://api2.branch.io/v1/url'

# ==== Firebase Admin 초기화 (Railway 환경변수 기반) ====
if not firebase_admin._apps:
    firebase_creds = {
        "type":                        os.environ["type"],
        "project_id":                  os.environ["project_id"],
        "private_key_id":              os.environ["private_key_id"],
        "private_key":                 os.environ["private_key"].replace('\\n', '\n'),
        "client_email":                os.environ["client_email"],
        "client_id":                   os.environ["client_id"],
        "auth_uri":                    os.environ["auth_uri"],
        "token_uri":                   os.environ["token_uri"],
        "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
        "client_x509_cert_url":        os.environ["client_x509_cert_url"]
    }
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)

# ==== Flask 앱 설정 ====
app = Flask(__name__)
app.config['UPLOAD_FOLDER']       = 'static'
app.config['MAX_CONTENT_LENGTH']  = 2 * 1024 * 1024 * 1024  # 2GB limit

# ==== Wasabi S3 클라이언트 설정 ====
s3 = boto3.client(
    's3',
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name           = REGION_NAME,
    endpoint_url          = f'https://s3.{REGION_NAME}.wasabisys.com'
)
config = TransferConfig(
    multipart_threshold = 1024 * 1024 * 25,
    multipart_chunksize = 1024 * 1024 * 50,
    max_concurrency     = 5,
    use_threads         = True
)

# ==== 1) pre-signed URL 생성 함수 ====
def generate_presigned_url(key, expires_in=3600*24):
    """
    S3 객체에 대한 pre-signed GET URL을 만듭니다.
    expires_in 초 후 만료됩니다.
    """
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

# ==== 2) Branch 딥링크 생성 함수 ====
def create_branch_link(deep_url):
    """
    Branch.io API를 호출하여 deep_url을 래핑한 딥링크를 생성합니다.
    """
    payload = {
        "branch_key": BRANCH_KEY,
        "campaign": "lecture_upload",
        "channel": "flask_server",
        "data": {
            "$desktop_url": deep_url,
            "$ios_url":     deep_url,
            "$android_url": deep_url,
            "lecture_url": deep_url
        }
    }
    res = requests.post(BRANCH_API_URL, json=payload)
    res.raise_for_status()
    return res.json().get('url')

# ==== 3) QR 코드 생성 (로고 포함) ====
def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    """
    link_url을 포함한 QR 코드를 만들고, 중앙에 logo를 삽입합니다.
    """
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H)
    qr.add_data(link_url)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    # 가운데 로고 삽입 (선택 사항)
    if os.path.exists(logo_path):
        try:
            logo = Image.open(logo_path)
            qr_w, qr_h = qr_img.size
            logo_size = int(qr_w * size_ratio)
            logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
            pos = ((qr_w - logo_size)//2, (qr_h - logo_size)//2)
            qr_img.paste(logo, pos, mask=logo if logo.mode=='RGBA' else None)
        except Exception as e:
            print(f"⚠️ QR 로고 삽입 실패: {e}")

    qr_img.save(output_path)

# ==== 메인 업로드 라우트 (/upload) ====
@app.route('/upload', methods=['POST'])
def upload_video():
    """
    1) 업로드된 비디오를 Wasabi S3에 저장
    2) pre-signed URL 생성
    3) Branch 딥링크 생성
    4) QR 코드 생성 및 S3에 업로드
    5) 로그(CSV)에 기록 후 JSON 응답
    """
    file      = request.files.get('file')
    group_name= request.form.get('group_name', 'default')

    if not file:
        return jsonify({"error": "file is required"}), 400

    # a) S3에 업로드
    date_str     = datetime.now().strftime('%Y%m%d')
    safe_name    = re.sub(r'[^\w]', '_', group_name)
    folder       = f"videos/{safe_name}_{date_str}"
    ext          = Path(file.filename).suffix or '.mp4'
    video_key    = f"{folder}/video{ext}"
    temp_path    = Path(f"./tmp{ext}")
    file.save(temp_path)
    s3.upload_file(str(temp_path), BUCKET_NAME, video_key,
                   Config=config)
    temp_path.unlink(missing_ok=True)

    # b) presigned URL
    presigned_url = generate_presigned_url(video_key)

    # c) Branch 딥링크
    branch_url    = create_branch_link(presigned_url)

    # d) QR 코드 생성 & S3 업로드
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(branch_url, local_qr)

    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    # e) CSV 로그 기록
    new_log = not os.path.exists(UPLOAD_LOG)
    with open(UPLOAD_LOG, 'a', newline='', encoding='utf-8') as csvf:
        writer = csv.writer(csvf)
        if new_log:
            writer.writerow(['video_key','presigned_url','branch_url','qr_key','timestamp'])
        writer.writerow([video_key, presigned_url, branch_url, qr_key, date_str])

    # f) 최종 JSON 반환
    return jsonify({
        "video_key":     video_key,
        "presigned_url": presigned_url,
        "branch_url":    branch_url,
        "qr_key":        qr_key,
    }), 200

# ==== 그룹 정보 조회 API (/api/group/<id>) ====
@app.route('/api/group/<group_id>')
def get_group_info(group_id):
    """
    upload_log.csv에서 group_id가 포함된 레코드를 찾아
    presigned_url과 branch_url을 반환합니다.
    """
    if not os.path.exists(UPLOAD_LOG):
        return jsonify({"error":"log missing"}), 404

    with open(UPLOAD_LOG, newline='', encoding='utf-8') as csvf:
        for row in csv.DictReader(csvf):
            if group_id in row['video_key']:
                return jsonify({
                    "presigned_url": row['presigned_url'],
                    "branch_url":    row['branch_url']
                })
    return jsonify({"error":"not found"}), 404

# ==== Firebase Custom Token API (/verifyNaver) ====
@app.route('/verifyNaver', methods=['POST'])
def verify_naver():
    access_token = request.form.get("accessToken")
    if not access_token:
        return jsonify({"error":"accessToken required"}), 400

    # Naver 프로필 조회
    res = requests.get("https://openapi.naver.com/v1/nid/me",
                       headers={"Authorization":f"Bearer {access_token}"})
    if res.status_code != 200:
        return jsonify({"error":"invalid token"}), 400

    naver_id = res.json().get("response", {}).get("id")
    uid      = f"naver:{naver_id}"
    custom_token = auth.create_custom_token(uid).decode('utf-8')
    return jsonify({"firebase_token": custom_token})

# ==== 서버 실행 ====
if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)

