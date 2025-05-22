import os
import uuid
import re
from pathlib import Path
from datetime import datetime, timedelta
from flask import (
    Flask, request, render_template,
    redirect, url_for, session, abort
)
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
import requests
from urllib.parse import urlparse, parse_qs

import firebase_admin
from firebase_admin import credentials, firestore

# ==== 환경변수 설정 ====
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', 'changeme')
AWS_ACCESS_KEY   = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY   = os.environ['AWS_SECRET_KEY']
REGION_NAME      = os.environ['REGION_NAME']
BUCKET_NAME      = os.environ['BUCKET_NAME']
APP_BASE_URL     = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
BRANCH_KEY       = os.environ['BRANCH_KEY']
BRANCH_API_URL   = 'https://api2.branch.io/v1/url'
SECRET_KEY       = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

# ==== Firebase Admin + Firestore 초기화 ====
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

# ==== 유틸리티 함수들 ====
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
        pos = ((qr_w-logo_size)//2, (qr_h-logo_size)//2)
        qr_img.paste(logo, pos, mask=(logo if logo.mode=='RGBA' else None))
    qr_img.save(output_path)

# ==== Presigned URL 유효성 검사 ====
def is_presigned_url_expired(url, safety_margin_minutes=60):
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if 'X-Amz-Date' not in query or 'X-Amz-Expires' not in query:
            return True

        issued_str = query['X-Amz-Date'][0]
        expires_in = int(query['X-Amz-Expires'][0])

        issued_time = datetime.strptime(issued_str, '%Y%m%dT%H%M%SZ')
        expiry_time = issued_time + timedelta(seconds=expires_in)
        margin_time = datetime.utcnow() + timedelta(minutes=safety_margin_minutes)

        return margin_time >= expiry_time
    except Exception as e:
        print(f"URL 검사 중 오류: {e}")
        return True

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

    main_cats = ['기계','공구','장비']
    sub_map   = {
        '기계': ['공작기계','제조기계','산업기계'],
        '공구': ['수공구','전동공구','절삭공구'],
        '장비': ['안전장비','운송장비','작업장비']
    }
    leaf_map = {
        '공작기계': ['불도저','크레인','굴착기'],
        '제조기계': ['사출 성형기','프레스기','열성형기'],
        '산업기계': ['CNC 선반','절삭기','연삭기'],
        '수공구':   ['드릴','해머','플라이어'],
        '전동공구': ['그라인더','전동 드릴','해머드릴'],
        '절삭공구': ['커터','플라즈마 노즐','드릴 비트'],
        '안전장비': ['헬멧','방진 마스크','낙하 방지벨트'],
        '운송장비': ['리프트 장비','체인 블록','호이스트'],
        '작업장비': ['스캐폴딩','작업대','리프트 테이블']
    }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map, leafs=leaf_map)

@app.route('/upload', methods=['POST'])
def upload_video():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file          = request.files.get('file')
    group_name    = request.form.get('group_name','default')
    main_cat      = request.form.get('main_category','')
    sub_cat       = request.form.get('sub_category','')
    leaf_cat      = request.form.get('sub_sub_category','')
    lecture_time  = request.form.get('time','')
    lecture_level = request.form.get('level','')
    lecture_tag   = request.form.get('tag','')

    if not file:
        return "파일이 필요합니다.", 400

    group_id  = uuid.uuid4().hex
    date_str  = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder    = f"videos/{group_id}_{safe_name}_{date_str}"
    ext       = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"
    tmp_path  = Path(f"./tmp{ext}")
    file.save(tmp_path)
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    presigned_url = generate_presigned_url(video_key, expires_in=604800)
    branch_url = create_branch_link(presigned_url, group_id)

    # QR에는 /watch/<group_id> 고정 링크를 사용
    fixed_watch_url = f"{APP_BASE_URL}{group_id}"
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(fixed_watch_url, local_qr)
    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    db.collection('uploads').document(group_id).set({
        'group_id':        group_id,
        'group_name':      group_name,
        'main_category':   main_cat,
        'sub_category':    sub_cat,
        'sub_sub_category': leaf_cat,
        'time':            lecture_time,
        'level':           lecture_level,
        'tag':             lecture_tag,
        'video_key':       video_key,
        'presigned_url':   presigned_url,
        'branch_url':      branch_url,
        'branch_updated_at': datetime.utcnow().isoformat(),
        'qr_key':          qr_key,
        'upload_date':     date_str
    })

    return render_template('success.html', group_id=group_id, main=main_cat, sub=sub_cat, leaf=leaf_cat, time=lecture_time, level=lecture_level, tag=lecture_tag, presigned_url=presigned_url, branch_url=branch_url, qr_url=url_for('static', filename=qr_filename))

@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    doc_ref = db.collection('uploads').document(group_id)
    doc = doc_ref.get()
    if not doc.exists:
        abort(404)
    data = doc.to_dict()

    current_presigned = data.get('presigned_url', '')
    current_branch_url = data.get('branch_url', '')
    should_renew = not current_presigned or is_presigned_url_expired(current_presigned, 60)

    if should_renew:
        new_presigned_url = generate_presigned_url(data['video_key'], expires_in=604800)
        new_branch_url = create_branch_link(new_presigned_url, group_id)
        doc_ref.update({
            'presigned_url': new_presigned_url,
            'branch_url': new_branch_url,
            'branch_updated_at': datetime.utcnow().isoformat()
        })
        video_url = new_presigned_url
    else:
        video_url = current_presigned

    return render_template('watch.html', video_url=video_url)

# ==== 서버 실행 ====
if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
