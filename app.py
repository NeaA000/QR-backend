import os
import uuid
import re
import tempfile
import logging
from pathlib import Path
from datetime import datetime, timedelta
from flask import (
    Flask, request, render_template,
    redirect, url_for, session, abort
)
from werkzeug.utils import secure_filename
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
import requests
from urllib.parse import urlparse, parse_qs

import firebase_admin
from firebase_admin import credentials, firestore
from moviepy.editor import VideoFileClip

# ==== 로깅 설정 ====
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==== 필수 환경변수 체크 ====
required_envs = [
    'AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'REGION_NAME', 'BUCKET_NAME',
    'BRANCH_KEY', 'FLASK_SECRET_KEY',
    'type', 'project_id', 'private_key_id', 'private_key',
    'client_email', 'client_id', 'auth_uri', 'token_uri',
    'auth_provider_x509_cert_url', 'client_x509_cert_url'
]
for var in required_envs:
    if var not in os.environ or not os.environ[var]:
        logger.error(f"필수 환경변수 '{var}' 가 누락되었습니다.")
        raise RuntimeError(f"Missing required environment variable: {var}")

# ==== 환경변수 ====
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', 'changeme')
AWS_ACCESS_KEY   = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY   = os.environ['AWS_SECRET_KEY']
REGION_NAME      = os.environ['REGION_NAME']
BUCKET_NAME      = os.environ['BUCKET_NAME']
APP_BASE_URL     = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
BRANCH_KEY       = os.environ['BRANCH_KEY']
BRANCH_API_URL   = 'https://api2.branch.io/v1/url'
SECRET_KEY       = os.environ['FLASK_SECRET_KEY']
DEBUG_MODE       = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'

# ==== Firebase Admin 초기화 ====
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

db = firestore.client()

# ==== Flask 앱 설정 ====
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER']      = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ==== Wasabi S3 설정 ====
s3 = boto3.client(
    's3',
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name           = REGION_NAME,
    endpoint_url          = f'https://s3.{REGION_NAME}.wasabisys.com'
)
config = TransferConfig(
    multipart_threshold = 25 * 1024 * 1024,
    multipart_chunksize = 50 * 1024 * 1024,
    max_concurrency     = 5,
    use_threads         = True
)

# ==== 유틸 함수 ====
def generate_presigned_url(key, expires_in=86400):
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def create_branch_link(deep_url, group_id):
    payload = {
        "branch_key": BRANCH_KEY,
        "campaign":   "lecture_upload",
        "channel":    "flask_server",
        "data": {
            "$desktop_url": deep_url,
            "$ios_url":     deep_url,
            "$android_url": deep_url,
            "$fallback_url": f"{APP_BASE_URL}{group_id}",
            "lecture_url":  deep_url
        }
    }
    res = requests.post(BRANCH_API_URL, json=payload)
    res.raise_for_status()
    return res.json().get('url')

def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H)
    qr.add_data(link_url)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    if os.path.exists(logo_path):
        logo = Image.open(logo_path)
        qr_w, qr_h = qr_img.size
        logo_size = int(qr_w * size_ratio)
        logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
        pos = ((qr_w - logo_size) // 2, (qr_h - logo_size) // 2)
        qr_img.paste(logo, pos, mask=(logo if logo.mode == 'RGBA' else None))
    qr_img.save(output_path)

def is_presigned_url_expired(url, safety_margin_minutes=60):
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if 'X-Amz-Date' not in query or 'X-Amz-Expires' not in query:
            return True
        issued_time = datetime.strptime(query['X-Amz-Date'][0], '%Y%m%dT%H%M%SZ')
        expires_in = int(query['X-Amz-Expires'][0])
        expiry_time = issued_time + timedelta(seconds=expires_in)
        margin_time = datetime.utcnow() + timedelta(minutes=safety_margin_minutes)
        return margin_time >= expiry_time
    except Exception as e:
        logger.warning(f"URL 검증 중 오류: {e}")
        return True

# ==== 라우트 정의 ====
@app.route('/', methods=['GET'])
def login_page():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    pw = request.form.get('password','')
    if pw == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('upload_form'))
    return render_template('login.html', error="비밀번호가 올바르지 않습니다.")

@app.route('/upload_form', methods=['GET'])
def upload_form():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    main_cats = ['기계','공구','장비']
    sub_map   = {
        '기계': ['공작기계','제조기계','산업기계'],
        '공구': ['수공구','전동공구','절삭공구'],
        '장비': ['안전장비','운송장비','작업장비']
    }
    leaf_map = { ... }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map, leafs=leaf_map)

@app.route('/upload', methods=['POST'])
def upload_video():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file = request.files.get('file')
    if not file:
        return "파일이 필요합니다.", 400

    group_name    = request.form.get('group_name','default')
    safe_group    = secure_filename(group_name) or uuid.uuid4().hex
    main_cat      = request.form.get('main_category','')
    sub_cat       = request.form.get('sub_category','')
    leaf_cat      = request.form.get('sub_sub_category','')
    lecture_time  = request.form.get('time','')
    lecture_level = request.form.get('level','')
    lecture_tag   = request.form.get('tag','')

    group_id = uuid.uuid4().hex
    date_str = datetime.utcnow().strftime('%Y%m%d')
    ext = Path(file.filename).suffix or '.mp4'

    # 임시 파일 생성
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    try:
        file.save(tmp_file.name)
        # 비디오 길이 추출
        with VideoFileClip(tmp_file.name) as clip:
            duration_sec = clip.duration
    finally:
        tmp_file.close()

    # S3 업로드
    folder    = f"videos/{group_id}_{safe_group}_{date_str}"
    video_key = f"{folder}/video{ext}"
    s3.upload_file(tmp_file.name, BUCKET_NAME, video_key, Config=config)
    os.unlink(tmp_file.name)

    presigned_url = generate_presigned_url(video_key, expires_in=604800)
    branch_url    = create_branch_link(presigned_url, group_id)

    # QR 코드 생성 및 업로드
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(branch_url, local_qr)
    qr_key      = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)
    os.remove(local_qr)

    # Firestore 기록
    db.collection('uploads').document(group_id).set({
        'group_id':          group_id,
        'group_name':        group_name,
        'main_category':     main_cat,
        'sub_category':      sub_cat,
        'sub_sub_category':  leaf_cat,
        'time':              lecture_time,
        'level':             lecture_level,
        'tag':               lecture_tag,
        'video_key':         video_key,
        'presigned_url':     presigned_url,
        'branch_url':        branch_url,
        'branch_updated_at': datetime.utcnow().isoformat(),
        'qr_key':            qr_key,
        'upload_date':       date_str,
        'durationSec':       duration_sec
    }, merge=True)

    return render_template(
        'success.html',
        group_id=group_id,
        main=main_cat,
        sub=sub_cat,
        leaf=leaf_cat,
        time=lecture_time,
        level=lecture_level,
        tag=lecture_tag,
        presigned_url=presigned_url,
        branch_url=branch_url,
        qr_url=url_for('static', filename=qr_filename)
    )

@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    doc_ref = db.collection('uploads').document(group_id)
    doc = doc_ref.get()
    if not doc.exists:
        abort(404)
    data = doc.to_dict()

    current_url = data.get('presigned_url', '')
    if not current_url or is_presigned_url_expired(current_url, 60):
        new_url    = generate_presigned_url(data['video_key'], expires_in=604800)
        new_branch = create_branch_link(new_url, group_id)
        doc_ref.update({
            'presigned_url':     new_url,
            'branch_url':        new_branch,
            'branch_updated_at': datetime.utcnow().isoformat()
        })
        video_url = new_url
    else:
        video_url = current_url

    return render_template('watch.html', video_url=video_url)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=DEBUG_MODE)
