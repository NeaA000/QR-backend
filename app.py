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

# ==== 사용자 설정 ====
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
REGION_NAME = os.environ.get('REGION_NAME')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
UPLOAD_LOG = 'upload_log.csv'

# ==== Firebase Admin 초기화 (Railway 환경변수 기반) ====
if not firebase_admin._apps:
    firebase_creds = {
        "type": os.environ["type"],
        "project_id": os.environ["project_id"],
        "private_key_id": os.environ["private_key_id"],
        "private_key": os.environ["private_key"].replace('\\n', '\n'),
        "client_email": os.environ["client_email"],
        "client_id": os.environ["client_id"],
        "auth_uri": os.environ["auth_uri"],
        "token_uri": os.environ["token_uri"],
        "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
        "client_x509_cert_url": os.environ["client_x509_cert_url"]
    }
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)

# ==== Flask 앱 설정 ====
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024

# ==== Wasabi S3 클라이언트 설정 ====
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=f'https://s3.{REGION_NAME}.wasabisys.com'
)

config = TransferConfig(
    multipart_threshold=1024 * 1024 * 25,
    multipart_chunksize=1024 * 1024 * 50,
    max_concurrency=5,
    use_threads=True
)

# ==== QR 코드 생성 함수 ====
def create_qr_with_logo(url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H)
    qr.add_data(url)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    if os.path.exists(logo_path):
        try:
            logo = Image.open(logo_path)
            qr_width, qr_height = qr_img.size
            logo_size = int(qr_width * size_ratio)
            logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
            pos = ((qr_width - logo_size) // 2, (qr_height - logo_size) // 2)
            qr_img.paste(logo, pos, mask=logo if logo.mode == 'RGBA' else None)
        except Exception as e:
            print(f"⚠️ 로고 이미지를 불러올 수 없습니다: {e}")

    qr_img.save(output_path)

# ==== 업로드 라우트 ====
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        group_name = request.form.get('group_name')
        file = request.files.get('file')

        if not group_name or not file:
            return "그룹 이름과 영상을 모두 업로드해주세요.", 400

        group_id = uuid.uuid4().hex
        date_str = datetime.now().strftime('%Y%m%d')
        safe_group_name = re.sub(r'[^\w가-힣]', '_', group_name)
        qr_filename = f"{safe_group_name}_{date_str}.png"
        local_qr_path = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
        qr_url = f"{APP_BASE_URL}{group_id}"
        s3_folder = f"groups/{group_name}_{date_str}"

        ext = file.filename.split('.')[-1]
        filename = f"video.{ext}"
        temp_path = Path(f"./temp_{filename}")
        file.save(temp_path)

        s3_key = f"{s3_folder}/{filename}"
        s3.upload_file(
            str(temp_path), BUCKET_NAME, s3_key,
            ExtraArgs={'ACL': 'public-read'},
            Config=config
        )

        try:
            temp_path.unlink()
        except PermissionError:
            print(f"⚠️ 파일 삭제 실패: {temp_path.name}")

        create_qr_with_logo(qr_url, local_qr_path)

        s3_qr_key = f"{s3_folder}/{qr_filename}"
        s3.upload_file(
            local_qr_path, BUCKET_NAME, s3_qr_key,
            ExtraArgs={'ACL': 'public-read'}
        )

        csv_exists = os.path.exists(UPLOAD_LOG)
        with open(UPLOAD_LOG, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            if not csv_exists:
                writer.writerow(['group_id', 'group_name', 's3_folder', 'video', 'qr_filename', 'qr_url', 'upload_date'])
            writer.writerow([
                group_id, group_name, s3_folder,
                filename,
                qr_filename, qr_url, date_str
            ])

        return f"""
        <h3>✅ 업로드 완료</h3>
        <p>Group: {group_name} ({group_id})</p>
        <p><a href='{qr_url}' target='_blank'>{qr_url}</a></p>
        <img src='/static/{qr_filename}' width='200'><br>
        <a href='/'>다시 업로드하기</a>
        """

    return render_template('index.html')

# ==== 그룹 정보 API ====
@app.route('/api/group/<group_id>')
def get_group_info(group_id):
    if not os.path.exists(UPLOAD_LOG):
        return jsonify({"error": "no log"}), 404

    with open(UPLOAD_LOG, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['group_id'] == group_id:
                return jsonify({
                    "group_id": row['group_id'],
                    "group_name": row['group_name'],
                    "video": f"https://s3.{REGION_NAME}.wasabisys.com/{BUCKET_NAME}/{row['s3_folder']}/{row['video']}"
                })

    return jsonify({"error": "not found"}), 404

# ==== Firebase Custom Token 발급 API ====
@app.route('/verifyNaver', methods=['POST'])
def verify_naver():
    access_token = request.form.get("accessToken")
    if not access_token:
        return jsonify({"error": "accessToken required"}), 400

    headers = {"Authorization": f"Bearer {access_token}"}
    res = requests.get("https://openapi.naver.com/v1/nid/me", headers=headers)

    if res.status_code != 200:
        return jsonify({"error": "invalid access token"}), 400

    user_info = res.json().get("response")
    naver_id = user_info.get("id")
    uid = f"naver:{naver_id}"

    try:
        custom_token = auth.create_custom_token(uid)
        return jsonify({"firebase_token": custom_token.decode('utf-8')})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==== 서버 실행 ====
if __name__ == '__main__':
    app.run(debug=True)
