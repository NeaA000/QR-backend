import os
import csv
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

# ==== .env 로딩 ====
load_dotenv()

# ==== Flask 기본 설정 ====
UPLOAD_LOG = 'upload_log.csv'
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret")
app.config['UPLOAD_FOLDER'] = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 최대 2GB

# ==== 환경 변수 ====
ADMIN_ID = os.getenv("ADMIN_ID", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
REGION_NAME = os.getenv("REGION_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
APP_BASE_URL = os.getenv("APP_BASE_URL")
WASABI_ENDPOINT = f"https://s3.{REGION_NAME}.wasabisys.com"
WASABI_HOSTING_BASE = f"https://{BUCKET_NAME}.s3.{REGION_NAME}.wasabisys.com"

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

# ==== 관리자 로그인 ====
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

# ==== 업로드 폼 + QR 생성 ====
@app.route('/', methods=['GET', 'POST'])
def upload():
    if not session.get('admin'):
        return redirect('/login')

    if request.method == 'POST':
        group_name = request.form.get('group_name')
        files = request.files.getlist('files')

        if not group_name or len(files) != 2:
            return "그룹 이름과 영상 2개를 모두 업로드해주세요.", 400

        group_id = uuid.uuid4().hex
        date_str = datetime.now().strftime('%Y%m%d')
        safe_name = re.sub(r'[^\w\uac00-\ud7a3]', '_', group_name)
        qr_filename = f"{safe_name}_{date_str}.png"
        tmp_qr_path = f"/tmp/{qr_filename}"
        qr_url = f"{APP_BASE_URL}/{group_id}"
        s3_folder = f"groups/{group_name}_{date_str}"

        uploaded_files = []
        for idx, file in enumerate(files):
            ext = file.filename.split('.')[-1]
            filename = f"video{idx + 1}.{ext}"
            tmp_path = Path(f"/tmp/temp_{filename}")
            file.save(tmp_path)

            s3_key = f"{s3_folder}/{filename}"
            s3.upload_file(
                str(tmp_path), BUCKET_NAME, s3_key,
                ExtraArgs={'ACL': 'public-read'},
                Config=config
            )
            tmp_path.unlink(missing_ok=True)
            uploaded_files.append(filename)

        create_qr_with_logo(qr_url, tmp_qr_path)

        s3.upload_file(tmp_qr_path, BUCKET_NAME, f"{s3_folder}/{qr_filename}",
                       ExtraArgs={'ACL': 'public-read'})
        os.remove(tmp_qr_path)

        write_log(group_id, group_name, s3_folder, uploaded_files, qr_filename, qr_url, date_str)

        qr_img_url = f"{WASABI_HOSTING_BASE}/{quote(s3_folder)}/{qr_filename}"

        return f"""
        <h3>✅ 업로드 완료</h3>
        <p>Group: {group_name} ({group_id})</p>
        <p><a href='{qr_url}' target='_blank'>{qr_url}</a></p>
        <img src='{qr_img_url}' width='200'><br>
        <a href='/'>다시 업로드하기</a>
        """

    return render_template('upload.html')

# ==== QR 생성 함수 ====
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

# ==== 업로드 로그 기록 ====
def write_log(group_id, name, folder, files, qr_file, qr_url, date):
    csv_exists = os.path.exists(UPLOAD_LOG)
    with open(UPLOAD_LOG, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not csv_exists:
            writer.writerow(['group_id', 'group_name', 's3_folder', 'video1', 'video2', 'qr_filename', 'qr_url', 'upload_date'])
        writer.writerow([group_id, name, folder, files[0], files[1], qr_file, qr_url, date])

# ==== 사용자 앱에서 호출하는 API ====
@app.route('/api/group/<group_id>')
def api_group(group_id):
    try:
        with open(UPLOAD_LOG, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['group_id'] == group_id:
                    folder = row['s3_folder']
                    v1 = f"{WASABI_HOSTING_BASE}/{quote(folder)}/{row['video1']}"
                    v2 = f"{WASABI_HOSTING_BASE}/{quote(folder)}/{row['video2']}"
                    return jsonify({
                        "groupName": row['group_name'],
                        "video1": v1,
                        "video2": v2
                    })
        return jsonify({"error": "Group not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==== 로그아웃 ====
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)