import json
import os
import uuid
import re
from pathlib import Path
from datetime import datetime
from flask import Flask, request, redirect, render_template, session, url_for, jsonify
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
from urllib.parse import quote
from dotenv import load_dotenv
import requests
from unidecode import unidecode
import firebase_admin
from firebase_admin import credentials, firestore

# ==== .env 로드 ====
load_dotenv()

# ==== Firebase Admin 초기화 ====
firebase_config = {
    "type": os.getenv("type"),
    "project_id": os.getenv("project_id"),
    "private_key_id": os.getenv("private_key_id"),
    "private_key": os.getenv("private_key").replace("\\n", "\n"),
    "client_email": os.getenv("client_email"),
    "client_id": os.getenv("client_id"),
    "auth_uri": os.getenv("auth_uri"),
    "token_uri": os.getenv("token_uri"),
    "auth_provider_x509_cert_url": os.getenv("auth_provider_x509_cert_url"),
    "client_x509_cert_url": os.getenv("client_x509_cert_url"),
    "universe_domain": os.getenv("universe_domain"),
}
with open("firebase_key.json", "w") as f:
    json.dump(firebase_config, f)

cred = credentials.Certificate("firebase_key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# ==== Flask 설정 ====
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret")
app.config['UPLOAD_FOLDER'] = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024

# ==== 환경 변수 ====
ADMIN_ID = os.getenv("ADMIN_ID", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
REGION_NAME = os.getenv("REGION_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
APP_BASE_URL = os.getenv("APP_BASE_URL")
WASABI_ENDPOINT = f"https://s3.{REGION_NAME}.wasabisys.com"
WASABI_HOSTING_BASE = f"https://{BUCKET_NAME}.s3.{REGION_NAME}.wasabisys.com"
BRANCH_KEY = os.getenv("BRANCH_KEY")

# ==== S3 클라이언트 ====
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=REGION_NAME,
    endpoint_url=WASABI_ENDPOINT
)
config = TransferConfig(
    multipart_threshold=1024 * 1024 * 25,
    multipart_chunksize=1024 * 1024 * 50,
    max_concurrency=5,
    use_threads=True
)

# ==== Branch 딥링크 생성 ====
def create_branch_link(group_id, group_name):
    group_slug = unidecode(group_name).replace(" ", "_")
    date_str = datetime.now().strftime('%Y%m%d')
    alias = f"video_{group_slug}_{date_str}"
    payload = {
        "branch_key": BRANCH_KEY,
        "campaign": "qr_video",
        "channel": "qr",
        "feature": "video_upload",
        "alias": alias,
        "data": {
            "group_id": group_id,
            "group_name": group_name,
            "type": "video",
            "$fallback_url": f"{APP_BASE_URL}/video_{group_slug}_{date_str}"
        }
    }
    try:
        r = requests.post("https://api2.branch.io/v1/url", json=payload)
        if r.status_code == 200:
            return r.json().get("url")
    except Exception as e:
        print("[BRANCH] Error:", e)
    return f"{APP_BASE_URL}/info"

# ==== alias → group_id API ====
@app.route('/api/alias/<alias>')
def alias_to_group(alias):
    try:
        docs = db.collection("uploads").stream()
        for doc in docs:
            data = doc.to_dict()
            qr_url = data.get("qr_url")
            if qr_url and qr_url.endswith(f"/{alias}"):
                return jsonify({"group_id": data.get("group_id")})
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==== fallback 안내 페이지 ====
@app.route('/video_<slug>')
def video_redirect(slug):
    return f"""
    <h3>📱 이 콘텐츠는 모바일 앱에서 재생됩니다</h3>
    <p>슬러그: <b>{slug}</b></p>
    <p>앱이 설치되어 있다면 자동으로 열립니다.<br>
    설치되지 않았다면 이 페이지가 fallback 안내를 합니다.</p>
    """

# ==== 로그인 ====
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        id_input = request.form.get('id')
        pw_input = request.form.get('password')
        if id_input == ADMIN_ID and pw_input == ADMIN_PASSWORD:
            session['admin'] = True
            return redirect(url_for('upload'))
        else:
            return render_template('login.html', error="❌ 아이디 또는 비밀번호가 틀렸습니다.")
    return render_template('login.html')

# ==== 업로드 ====
@app.route('/', methods=['GET', 'POST'])
def upload():
    if not session.get('admin'):
        return redirect('/login')

    if request.method == 'POST':
        group_name = request.form.get('group_name')
        files = request.files.getlist('files')

        if not group_name or len(files) < 1:
            return "그룹 이름과 최소 1개 이상의 영상을 업로드해주세요.", 400

        group_id = "v_" + uuid.uuid4().hex
        date_str = datetime.now().strftime('%Y%m%d')
        safe_name = re.sub(r'[^\w]', '_', group_name)
        s3_folder = f"groups/{safe_name}_{date_str}"
        qr_filename = f"{safe_name}_{date_str}.png"
        tmp_qr_path = f"/tmp/{qr_filename}"

        uploaded_files = []
        for idx, file in enumerate(files):
            ext = file.filename.split('.')[-1]
            filename = f"video{idx + 1}.{ext}"
            tmp_path = Path(f"/tmp/temp_{filename}")
            file.save(tmp_path)
            s3_key = f"{s3_folder}/{filename}"
            s3.upload_file(str(tmp_path), BUCKET_NAME, s3_key, ExtraArgs={'ACL': 'public-read'}, Config=config)
            tmp_path.unlink(missing_ok=True)
            uploaded_files.append(filename)

        qr_url = create_branch_link(group_id, group_name)
        create_qr_with_logo(qr_url, tmp_qr_path)
        s3.upload_file(tmp_qr_path, BUCKET_NAME, f"{s3_folder}/{qr_filename}", ExtraArgs={'ACL': 'public-read'})
        os.remove(tmp_qr_path)

        db.collection("uploads").document(group_id).set({
            "group_id": group_id,
            "group_name": group_name,
            "s3_folder": s3_folder,
            "video1": uploaded_files[0] if len(uploaded_files) > 0 else None,
            "video2": uploaded_files[1] if len(uploaded_files) > 1 else None,
            "qr_filename": qr_filename,
            "qr_url": qr_url,
            "upload_date": date_str
        })

        qr_img_url = f"{WASABI_HOSTING_BASE}/{quote(s3_folder)}/{quote(qr_filename)}"
        return f"""
        <h3>✅ 업로드 완료</h3>
        <p>Group: {group_name} ({group_id})</p>
        <p><a href="{qr_url}" target="_blank">{qr_url}</a></p>
        <img src="{qr_img_url}" width="200"><br>
        <a href="/">다시 업로드하기</a>
        """

    return render_template('upload.html')

# ==== QR 코드 생성 ====
def create_qr_with_logo(url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H)
    qr.add_data(url)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    if os.path.exists(logo_path):
        logo = Image.open(logo_path)
        size = int(qr_img.size[0] * size_ratio)
        logo = logo.resize((size, size), Image.LANCZOS)
        pos = ((qr_img.size[0] - size) // 2, (qr_img.size[1] - size) // 2)
        qr_img.paste(logo, pos, mask=logo if logo.mode == 'RGBA' else None)
    qr_img.save(output_path)

# ==== 로그아웃 ====
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

# ==== 실행 ====
if __name__ == '__main__':
    try:
        os.remove("firebase_key.json")  # 보안상 삭제
    except FileNotFoundError:
        pass
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
