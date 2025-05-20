import os
import csv
import uuid
import re
from pathlib import Path
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, session, jsonify
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
import requests
import firebase_admin
from firebase_admin import credentials, auth

# ==== 환경변수 설정 ====
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', 'changeme')
AWS_ACCESS_KEY   = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY   = os.environ['AWS_SECRET_KEY']
REGION_NAME      = os.environ['REGION_NAME']
BUCKET_NAME      = os.environ['BUCKET_NAME']
APP_BASE_URL     = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
UPLOAD_LOG       = os.environ.get('UPLOAD_LOG_PATH', 'upload_log.csv')
BRANCH_KEY       = os.environ['BRANCH_KEY']
BRANCH_API_URL   = 'https://api2.branch.io/v1/url'
SECRET_KEY       = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

# ==== Firebase Admin 초기화 (생략 가능) ====
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
app.secret_key                  = SECRET_KEY
app.config['UPLOAD_FOLDER']     = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB

# ==== Wasabi S3 클라이언트 ====
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

# —– 유틸리티 함수들 —–

def generate_presigned_url(key, expires_in=86400):
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def create_branch_link(deep_url):
    payload = {
        "branch_key": BRANCH_KEY,
        "campaign":   "lecture_upload",
        "channel":    "flask_server",
        "data": {
            "$desktop_url": deep_url,
            "$ios_url":     deep_url,
            "$android_url": deep_url,
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
        pos = ((qr_w-logo_size)//2, (qr_h-logo_size)//2)
        qr_img.paste(logo, pos, mask=(logo if logo.mode=='RGBA' else None))
    qr_img.save(output_path)

# ==== 라우팅 ====

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
    # 예시 대/소분류
    main_cats = ['기계','공구','장비']
    sub_map   = {
        '기계':['공작기계','제조기계','산업기계'],
        '공구':['수공구','전동공구','절삭공구'],
        '장비':['안전장비','운송장비','작업장비']
    }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map)

@app.route('/upload', methods=['POST'])
def upload_video():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    # — 1) 폼 데이터 수신 —
    file          = request.files.get('file')
    group_name    = request.form.get('group_name','default')
    main_cat      = request.form.get('main_category','')
    sub_cat       = request.form.get('sub_category','')
    lecture_time  = request.form.get('time','')
    lecture_level = request.form.get('level','')
    lecture_tag   = request.form.get('tag','')

    if not file:
        return "파일이 필요합니다.", 400

    # — 2) Wasabi S3 업로드 —
    date_str  = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder    = f"videos/{safe_name}_{date_str}"
    ext       = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"
    tmp_path  = Path(f"./tmp{ext}")
    file.save(tmp_path)
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    # — 3) presigned URL 생성 —
    presigned_url = generate_presigned_url(video_key)

    # — 4) Branch 딥링크 생성 —
    branch_url = create_branch_link(presigned_url)

    # — 5) QR 코드 생성 & S3 업로드 —
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(branch_url, local_qr)
    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    # — 6) CSV 로그 기록 —
    header = ['video_key','main_category','sub_category','time','level','tag',
              'presigned_url','branch_url','qr_key','timestamp']
    newfile = not os.path.exists(UPLOAD_LOG)
    with open(UPLOAD_LOG, 'a', newline='', encoding='utf-8') as csvf:
        w = csv.writer(csvf)
        if newfile:
            w.writerow(header)
        w.writerow([
            video_key, main_cat, sub_cat, lecture_time, lecture_level, lecture_tag,
            presigned_url, branch_url, qr_key, date_str
        ])

    # — 7) 업로드 성공 페이지 렌더링 —
    return render_template('success.html',
        main=main_cat, sub=sub_cat,
        time=lecture_time, level=lecture_level, tag=lecture_tag,
        presigned_url=presigned_url,
        branch_url=branch_url,
        qr_url=url_for('static', filename=qr_filename)
    )

# ==== 서버 실행 ====
if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)
