# backend/app.py

import os
import uuid
import re
import io
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, date

from flask import (
    Flask, request, render_template,
    redirect, url_for, session, abort, jsonify
)
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
from urllib.parse import urlparse, parse_qs
import requests
import zipfile
import jwt  # PyJWT
from functools import wraps

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage

# ── 백그라운드 스케줄러 추가 ──
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import threading

# ── 변경된 부분: video 파일 길이를 가져오기 위한 import (MoviePy 최신 경로) ──
from moviepy.video.io.VideoFileClip import VideoFileClip

# ==== 환경변수 설정 ====
ADMIN_EMAIL       = os.environ.get('ADMIN_EMAIL', '')
ADMIN_PASSWORD    = os.environ.get('ADMIN_PASSWORD', 'changeme')
JWT_SECRET        = os.environ.get('JWT_SECRET', 'supersecretjwt')
JWT_ALGORITHM     = 'HS256'
JWT_EXPIRES_HOURS = 4

AWS_ACCESS_KEY    = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY    = os.environ['AWS_SECRET_KEY']
REGION_NAME       = os.environ['REGION_NAME']
BUCKET_NAME       = os.environ['BUCKET_NAME']
APP_BASE_URL      = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
SECRET_KEY        = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

# ==== Firebase Admin + Firestore + Storage 초기화 ====
if not firebase_admin._apps:
    firebase_creds = {
        "type":                        os.environ["type"],
        "project_id":                  os.environ["project_id"],
        "private_key_id":              os.environ["private_key_id"],
        "private_key":                 os.environ["private_key"].replace('\\n','\n'),
        "client_email":                os.environ["client_email"],
        "client_id":                   os.environ["client_id"],
        "auth_uri":                    os.environ["auth_uri"],
        "token_uri":                   os.environ["token_uri"],
        "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
        "client_x509_cert_url":        os.environ["client_x509_cert_url"]
    }
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred, {
        'storageBucket': f"{os.environ['project_id']}.appspot.com"
    })

db     = firestore.client()
bucket = storage.bucket()  # Firebase Storage 기본 버킷

# ==== Flask 앱 설정 ====
app = Flask(__name__)
app.secret_key                   = SECRET_KEY
app.config['UPLOAD_FOLDER']      = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB 상한
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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

# ==== 유틸리티 함수들 ====

def generate_presigned_url(key, expires_in=86400):
    """
    S3 객체에 대해 presigned URL 생성
    expires_in: URL 유효 기간(초)
    """
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    """
    QR 코드 생성 후 중앙에 로고 삽입
    """
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
    """
    presigned URL 만료 여부 확인
    safety_margin_minutes: 만료 전에 안전 여유 시간(분)
    """
    try:
        parsed      = urlparse(url)
        query       = parse_qs(parsed.query)
        if 'X-Amz-Date' not in query or 'X-Amz-Expires' not in query:
            return True
        issued_str  = query['X-Amz-Date'][0]
        expires_in  = int(query['X-Amz-Expires'][0])
        issued_time = datetime.strptime(issued_str, '%Y%m%dT%H%M%SZ')
        expiry_time = issued_time + timedelta(seconds=expires_in)
        margin_time = datetime.utcnow() + timedelta(minutes=safety_margin_minutes)
        return margin_time >= expiry_time
    except Exception as e:
        app.logger.warning(f"URL 검사 중 오류: {e}")
        return True

def parse_iso_week(week_str: str):
    """
    week_str 형식: "YYYY-Www" (예: "2025-W23")
    → 해당 ISO 주의 월요일 00:00:00 ~ 일요일 23:59:59 (UTC) 반환
    """
    try:
        year_part, week_part = week_str.split('-W')
        year     = int(year_part)
        week_num = int(week_part)
        week_start_date = date.fromisocalendar(year, week_num, 1)  # 월요일
        week_end_date   = week_start_date + timedelta(days=6)      # 일요일

        week_start_dt = datetime.combine(week_start_date, datetime.min.time())
        week_end_dt   = datetime.combine(week_end_date,   datetime.max.time())
        return week_start_dt, week_end_dt
    except Exception as e:
        raise ValueError(f"잘못된 week_str 형식: {week_str} ({e})")

def create_jwt_for_admin():
    """
    관리자 로그인 시 JWT 발급
    - payload에 발급 시간, 만료 시간, 식별자로 admin_email 포함
    """
    now      = datetime.utcnow()
    payload  = {
        'sub': ADMIN_EMAIL,
        'iat': now,
        'exp': now + timedelta(hours=JWT_EXPIRES_HOURS)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token

def verify_jwt_token(token: str) -> bool:
    """
    JWT 토큰 검증
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get('sub') == ADMIN_EMAIL
    except jwt.ExpiredSignatureError:
        return False
    except Exception:
        return False

def admin_required(f):
    """
    데코레이터: 요청 헤더에 'Authorization: Bearer <JWT>'가 있어야 접근 허용
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization', None)
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': '관리자 인증 필요'}), 401

        token = auth_header.split(' ', 1)[1]
        if not verify_jwt_token(token):
            return jsonify({'error': '유효하지 않은 또는 만료된 토큰'}), 401

        return f(*args, **kwargs)
    return decorated

# ===================================================================
# 새로 추가된 백그라운드 자동 갱신 시스템
# ===================================================================

def refresh_expiring_urls():
    """
    만료 임박한 presigned URL들을 일괄 갱신하는 백그라운드 작업
    - 2시간(120분) 여유를 두고 미리 갱신
    """
    try:
        app.logger.info("🔄 백그라운드 URL 갱신 작업 시작...")
        
        # Firestore에서 모든 업로드 문서 조회
        uploads_ref = db.collection('uploads')
        docs = uploads_ref.stream()
        
        updated_count = 0
        total_count = 0
        
        for doc in docs:
            total_count += 1
            data = doc.to_dict()
            
            current_url = data.get('presigned_url', '')
            video_key = data.get('video_key', '')
            
            if not video_key:
                app.logger.warning(f"⚠️  문서 {doc.id}에 video_key가 없습니다.")
                continue
            
            # URL이 없거나 만료 임박(2시간 여유) 시 갱신
            if not current_url or is_presigned_url_expired(current_url, safety_margin_minutes=120):
                try:
                    # 새 presigned URL 생성 (7일 유효)
                    new_presigned_url = generate_presigned_url(video_key, expires_in=604800)
                    
                    # Firestore 업데이트
                    doc.reference.update({
                        'presigned_url': new_presigned_url,
                        'auto_updated_at': datetime.utcnow().isoformat(),
                        'auto_update_reason': 'background_refresh'
                    })
                    
                    updated_count += 1
                    app.logger.info(f"✅ 문서 {doc.id} URL 갱신 완료")
                    
                except Exception as update_error:
                    app.logger.error(f"❌ 문서 {doc.id} URL 갱신 실패: {update_error}")
        
        app.logger.info(f"🎉 백그라운드 URL 갱신 완료: {updated_count}/{total_count} 개 갱신됨")
        
    except Exception as e:
        app.logger.error(f"❌ 백그라운드 URL 갱신 작업 중 오류: {e}")

def refresh_qr_presigned_urls():
    """
    QR 이미지의 presigned URL도 갱신 (선택사항)
    """
    try:
        app.logger.info("🔄 QR 이미지 URL 갱신 작업 시작...")
        
        uploads_ref = db.collection('uploads')
        docs = uploads_ref.stream()
        
        updated_count = 0
        
        for doc in docs:
            data = doc.to_dict()
            qr_key = data.get('qr_key', '')
            
            if not qr_key:
                continue
                
            try:
                # QR 이미지용 새 presigned URL 생성 (7일 유효)
                new_qr_url = generate_presigned_url(qr_key, expires_in=604800)
                
                doc.reference.update({
                    'qr_presigned_url': new_qr_url,
                    'qr_updated_at': datetime.utcnow().isoformat()
                })
                
                updated_count += 1
                
            except Exception as qr_error:
                app.logger.error(f"❌ QR URL 갱신 실패 {doc.id}: {qr_error}")
        
        app.logger.info(f"🎉 QR URL 갱신 완료: {updated_count}개")
        
    except Exception as e:
        app.logger.error(f"❌ QR URL 갱신 작업 중 오류: {e}")

# ===================================================================
# 스케줄러 설정 및 시작
# ===================================================================

# 스케줄러 인스턴스 생성
scheduler = BackgroundScheduler(
    timezone='UTC',
    job_defaults={
        'coalesce': True,  # 같은 작업이 중복 실행되지 않도록
        'max_instances': 1  # 최대 1개 인스턴스만 실행
    }
)

def start_background_scheduler():
    """
    백그라운드 스케줄러 시작
    """
    try:
        # 1. 동영상 URL 갱신 작업 (3시간마다)
        scheduler.add_job(
            func=refresh_expiring_urls,
            trigger=IntervalTrigger(hours=3),
            id='refresh_video_urls',
            name='동영상 URL 자동 갱신',
            replace_existing=True
        )
        
        # 2. QR 이미지 URL 갱신 작업 (6시간마다)
        scheduler.add_job(
            func=refresh_qr_presigned_urls,
            trigger=IntervalTrigger(hours=6),
            id='refresh_qr_urls',
            name='QR 이미지 URL 자동 갱신',
            replace_existing=True
        )
        
        # 스케줄러 시작
        scheduler.start()
        app.logger.info("🚀 백그라운드 URL 자동 갱신 스케줄러가 시작되었습니다.")
        app.logger.info("   - 동영상 URL: 3시간마다 갱신")
        app.logger.info("   - QR 이미지 URL: 6시간마다 갱신")
        
        # 앱 종료 시 스케줄러도 함께 종료
        atexit.register(lambda: scheduler.shutdown())
        
    except Exception as e:
        app.logger.error(f"❌ 스케줄러 시작 실패: {e}")

# ===================================================================
# 수동 갱신 API (관리자용)
# ===================================================================

@app.route('/api/admin/refresh-urls', methods=['POST'])
@admin_required
def manual_refresh_urls():
    """
    관리자가 수동으로 URL 갱신을 트리거할 수 있는 엔드포인트
    """
    try:
        # 백그라운드에서 실행하여 응답 지연 방지
        thread = threading.Thread(target=refresh_expiring_urls)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'message': 'URL 갱신 작업이 백그라운드에서 시작되었습니다.',
            'status': 'started'
        }), 200
        
    except Exception as e:
        app.logger.error(f"수동 URL 갱신 실패: {e}")
        return jsonify({'error': '갱신 작업 시작에 실패했습니다.'}), 500

@app.route('/api/admin/scheduler-status', methods=['GET'])
@admin_required
def get_scheduler_status():
    """
    스케줄러 상태 확인용 엔드포인트
    """
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return jsonify({
            'running': scheduler.running,
            'jobs': jobs
        }), 200
        
    except Exception as e:
        app.logger.error(f"스케줄러 상태 조회 실패: {e}")
        return jsonify({'error': '스케줄러 상태를 가져올 수 없습니다.'}), 500

# ===================================================================
# 업로드 핸들러: 동영상 길이를 자동으로 계산하여 lecture_time에 저장
# ===================================================================
@app.route('/upload', methods=['POST'])
def upload_video():
    """
    동영상 업로드 처리:
    1) 클라이언트에서 파일과 기타 메타데이터 수신
    2) 파일을 임시로 저장 → S3 업로드
    3) moviepy 로 동영상 길이(초 단위) 계산 → "분:초" 문자열로 변환
    4) Firestore에 group_id, lecture_time 등을 자동 저장
    """
    # 세션 인증(기존 로직)
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file          = request.files.get('file')
    group_name    = request.form.get('group_name', 'default')
    main_cat      = request.form.get('main_category', '')
    sub_cat       = request.form.get('sub_category', '')
    leaf_cat      = request.form.get('sub_sub_category', '')

    # lecture_time 은 더 이상 폼으로 받지 않고, 자동 계산
    lecture_level = request.form.get('level', '')
    lecture_tag   = request.form.get('tag', '')

    if not file:
        return "파일이 필요합니다.", 400

    # 1) 그룹 ID 생성 및 S3 키 구성
    group_id = uuid.uuid4().hex
    date_str = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder = f"videos/{group_id}_{safe_name}_{date_str}"
    ext = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"

    # 2) 임시 저장 및 S3 업로드
    tmp_path = Path(tempfile.gettempdir()) / f"{group_id}{ext}"
    file.save(tmp_path)

    # 3) moviepy 를 사용해 동영상 길이 계산
    try:
        with VideoFileClip(str(tmp_path)) as clip:
            duration_sec = int(clip.duration)  # 초 단위
        # with 블록을 벗어나면 clip.close()가 자동 호출됩니다.
    except Exception as e:
        duration_sec = 0
        app.logger.warning(f"moviepy 로 동영상 길이 가져오기 실패: {e}")

    # "분:초" 형식으로 변환 (예: 125초 → "2:05")
    minutes = duration_sec // 60
    seconds = duration_sec % 60
    lecture_time = f"{minutes}:{seconds:02d}"  # 예: "2:05"
    app.logger.info(f"계산된 동영상 길이: {lecture_time} (총 {duration_sec}초)")

    # S3 업로드
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    # 4) Presigned URL 생성 (7일 유효)
    presigned_url = generate_presigned_url(video_key, expires_in=604800)

    # 5) QR 링크 생성 및 S3 업로드
    qr_link = f"{APP_BASE_URL}{group_id}"
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(qr_link, local_qr)
    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    # 6) Firestore 메타데이터 저장 (lecture_time 필드에 자동 계산값 사용)
    db.collection('uploads').document(group_id).set({
        'group_id':         group_id,
        'group_name':       group_name,
        'main_category':    main_cat,
        'sub_category':     sub_cat,
        'sub_sub_category': leaf_cat,
        'time':             lecture_time,       # ← 자동 계산된 값
        'level':            lecture_level,
        'tag':              lecture_tag,
        'video_key':        video_key,
        'presigned_url':    presigned_url,
        'qr_link':          qr_link,
        'qr_key':           qr_key,
        'upload_date':      date_str,
        'auto_updated_at':  datetime.utcnow().isoformat(),  # 자동 갱신 추적용
        'auto_update_reason': 'initial_upload'
    })

    return render_template(
        'success.html',
        group_id      = group_id,
        main          = main_cat,
        sub           = sub_cat,
        leaf          = leaf_cat,
        time          = lecture_time,    # 뷰에도 자동으로 보여줄 수 있습니다
        level         = lecture_level,
        tag           = lecture_tag,
        presigned_url = presigned_url,
        qr_link       = qr_link,
        qr_url        = url_for('static', filename=qr_filename)
    )

# ===================================================================
# 새로 추가된 부분: 수료증 정보 생성 시 Firestore에 readyForExcel & excelUpdated 플래그 추가
# (Flutter나 다른 클라이언트가 수료증을 발급할 때 이 엔드포인트 호출)
# ===================================================================
@app.route('/create_certificate', methods=['POST'])
def create_certificate():
    """
    클라이언트(Flutter 등)에서 수료증을 발급할 때 호출.
    1) user_uid, cert_id, lectureTitle, pdfUrl 등을 JSON 바디로 전달
    2) Firestore에 새 문서를 생성하면서
       excelUpdated: False, readyForExcel: True 플래그를 함께 설정
    """
    data = request.get_json() or {}
    user_uid      = data.get('user_uid')
    cert_id       = data.get('cert_id')
    lecture_title = data.get('lectureTitle', '')
    pdf_url       = data.get('pdfUrl', '')

    if not user_uid or not cert_id or not pdf_url:
        return jsonify({'error': 'user_uid, cert_id, lectureTitle, pdfUrl이 필요합니다.'}), 400

    # Firestore Timestamp로 자동 저장
    cert_ref = db.collection('users').document(user_uid) \
                 .collection('completedCertificates').document(cert_id)
    cert_ref.set({
        'lectureTitle':    lecture_title,
        'issuedAt':        firestore.SERVER_TIMESTAMP,
        'pdfUrl':          pdf_url,
        'excelUpdated':    False,
        'readyForExcel':   True
    }, merge=True)

    return jsonify({'message': '수료증이 생성되었습니다. 워커가 엑셀 업데이트 대상에 추가됩니다.'}), 200

# ===================================================================
# 수료증이 들어올 때마다 Master 엑셀에 자동으로 추가하는 엔드포인트
# ===================================================================
@app.route('/add_certificate_to_master', methods=['POST'])
def add_certificate_to_master():
    """
    사용자가 수료증을 발급(저장)한 후, 필요에 따라 호출.
    1) Firestore에서 user_uid, cert_id로 수료증 정보 조회
    2) Firebase Storage에 저장된 master_certificates.xlsx 다운로드 (없으면 새로 생성)
    3) Pandas로 DataFrame 로드 → 새로운 행 추가
    4) 수정된 엑셀을 Firebase Storage에 업로드(덮어쓰기)
    5) Firestore 문서에 excelUpdated=True, readyForExcel=False로 업데이트
    """
    data = request.get_json() or {}
    user_uid = data.get('user_uid')
    cert_id  = data.get('cert_id')

    if not user_uid or not cert_id:
        return jsonify({'error': 'user_uid와 cert_id가 필요합니다.'}), 400

    # 1) Firestore에서 해당 수료증 문서 조회
    cert_ref = db.collection('users').document(user_uid) \
                 .collection('completedCertificates').document(cert_id)
    cert_doc = cert_ref.get()
    if not cert_doc.exists:
        return jsonify({'error': '해당 수료증이 존재하지 않습니다.'}), 404

    cert_info = cert_doc.to_dict()
    # PDF URL 필수 확인
    pdf_url       = cert_info.get('pdfUrl', '')
    if not pdf_url:
        return jsonify({'error': 'PDF URL이 없습니다.'}), 400

    lecture_title = cert_info.get('lectureTitle', cert_id)
    issued_at     = cert_info.get('issuedAt')  # Firestore Timestamp

    # Firestore Timestamp → datetime 변환
    if hasattr(issued_at, 'to_datetime'):
        issued_dt = issued_at.to_datetime()
    else:
        issued_dt = datetime.utcnow()

    # 2) Firebase Storage에서 master_certificates.xlsx 다운로드 (없으면 빈 DataFrame 생성)
    master_blob_name = 'master_certificates.xlsx'
    master_blob = bucket.blob(master_blob_name)

    try:
        existing_bytes = master_blob.download_as_bytes()
        excel_buffer   = io.BytesIO(existing_bytes)
        df_master      = pd.read_excel(excel_buffer, engine='openpyxl')
    except Exception:
        # 파일이 없거나 읽기 실패 시: 빈 DataFrame 생성
        df_master = pd.DataFrame(columns=[
            '업데이트 날짜', '사용자 UID', '전화번호', '이메일',
            '사용자 이름', '강의 제목', '발급 일시', 'PDF URL'
        ])

    # 3) DataFrame에 새로운 행 추가 (append 대신 concat 사용)
    # 사용자 프로필 조회 (이름/전화/이메일), 필요시 빈 문자열 처리
    user_ref = db.collection("users").document(user_uid)
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

    updated_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    issued_str   = issued_dt.strftime("%Y-%m-%d %H:%M:%S")

    new_row = pd.DataFrame([{
        '업데이트 날짜': updated_date,
        '사용자 UID':    user_uid,
        '전화번호':      user_phone,
        '이메일':        user_email,
        '사용자 이름':   user_name,
        '강의 제목':     lecture_title,
        '발급 일시':     issued_str,
        'PDF URL':       pdf_url
    }])
    df_master = pd.concat([df_master, new_row], ignore_index=True)

    # 4) 수정된 DataFrame을 BytesIO 버퍼에 엑셀로 쓰기
    out_buffer = io.BytesIO()
    with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
        df_master.to_excel(writer, index=False, sheet_name="Certificates")
    out_buffer.seek(0)

    # Firebase Storage에 덮어쓰기
    try:
        master_blob.upload_from_file(
            out_buffer,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        app.logger.error(f"마스터 엑셀 업로드 실패: {e}")
        return jsonify({'error': '수정된 엑셀 저장 중 오류가 발생했습니다.'}), 500

    # 5) Firestore 문서에 excelUpdated=True, readyForExcel=False로 업데이트
    cert_ref.update({
        "excelUpdated": True,
        "readyForExcel": False
    })

    return jsonify({'message': '마스터 엑셀에 수료증 정보가 성공적으로 추가되었습니다.'}), 200

# ==== 라우팅 설정 ====

@app.route('/', methods=['GET'])
def login_page():
    """로그인 페이지 렌더링 (관리자용)"""
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    """
    (기존 세션 기반) 관리자 페이지 로그인.
    그러나 플러터 앱에서는 이 엔드포인트 대신 아래 /api/admin/login 을 사용하여 JWT를 발급받습니다.
    """
    pw = request.form.get('password', '')
    email = request.form.get('email', '')

    if email == ADMIN_EMAIL and pw == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('upload_form'))
    return render_template('login.html', error="이메일 또는 비밀번호가 올바르지 않습니다.")

@app.route('/api/admin/login', methods=['POST'])
def api_admin_login():
    """
    Flutter 관리자 로그인(JWT 발급용).
    Body: { "email": "...", "password": "..." }
    """
    data = request.get_json() or {}
    email    = data.get('email', '').strip()
    password = data.get('password', '')

    if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
        token = create_jwt_for_admin()
        return jsonify({'token': token}), 200
    else:
        return jsonify({'error': '관리자 인증 실패'}), 401

@app.route('/upload_form', methods=['GET'])
def upload_form():
    """
    (기존) 관리자가 웹에서 업로드 페이지 접근 시 세션 기반 인증
    """
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    main_cats = ['기계', '공구', '장비']
    sub_map = {
        '기계': ['공작기계', '제조기계', '산업기계'],
        '공구': ['수공구', '전동공구', '절삭공구'],
        '장비': ['안전장비', '운송장비', '작업장비']
    }
    leaf_map = {
        '공작기계': ['불도저', '크레인', '굴착기'],
        '제조기계': ['사출 성형기', '프레스기', '열성형기'],
        '산업기계': ['CNC 선반', '절삭기', '연삭기'],
        '수공구':   ['드릴', '해머', '플라이어'],
        '전동공구': ['그라인더', '전동 드릴', '해머드릴'],
        '절삭공구': ['커터', '플라즈마 노즐', '드릴 비트'],
        '안전장비': ['헬멧', '방진 마스크', '낙하 방지벨트'],
        '운송장비': ['리프트 장비', '체인 블록', '호이스트'],
        '작업장비': ['스캐폴딩', '작업대', '리프트 테이블']
    }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map, leafs=leaf_map)

@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    """
    동영상 시청 페이지 (Presigned URL 갱신 포함, 기존 세션 기반)
    """
    doc_ref = db.collection('uploads').document(group_id)
    doc     = doc_ref.get()
    if not doc.exists:
        abort(404)
    data = doc.to_dict()

    current_presigned = data.get('presigned_url', '')
    if not current_presigned or is_presigned_url_expired(current_presigned, 60):
        new_presigned_url = generate_presigned_url(data['video_key'], expires_in=604800)
        doc_ref.update({
            'presigned_url':     new_presigned_url,
            'branch_updated_at': datetime.utcnow().isoformat()
        })
        video_url = new_presigned_url
    else:
        video_url = current_presigned

    return render_template('watch.html', video_url=video_url)

@app.route('/generate_weekly_zip', methods=['GET'])
@admin_required
def generate_weekly_zip():
    """
    관리자(JWT) 인증 후 특정 주차 전체 수료증 ZIP 생성/조회
    - query param: week (예: "2025-W23")
    """
    week_param = request.args.get('week')
    if not week_param:
        today = datetime.utcnow().date()
        y, w, _ = today.isocalendar()
        week_param = f"{y}-W{str(w).zfill(2)}"

    zip_key = f"full/{week_param}.zip"

    # 1) S3에 ZIP 존재하면 presigned URL 반환
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=zip_key)
        presigned = generate_presigned_url(zip_key, expires_in=3600)
        return jsonify({
            'zipUrl': presigned,
            'generated': False,
            'week': week_param
        })
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            return abort(500, description=f"S3 오류: {e}")

    # 2) ZIP 없으면 Firestore에서 주차별 수료증 조회
    try:
        week_start_dt, week_end_dt = parse_iso_week(week_param)
    except ValueError as ex:
        return abort(400, description=str(ex))

    start_ts = firestore.Timestamp.from_datetime(week_start_dt)
    end_ts   = firestore.Timestamp.from_datetime(week_end_dt)

    cert_docs = db.collection_group('completedCertificates') \
                  .where('issuedAt', '>=', start_ts) \
                  .where('issuedAt', '<=', end_ts) \
                  .stream()

    tmp_zip_path = f"/tmp/{week_param}.zip"
    with zipfile.ZipFile(tmp_zip_path, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        found_any = False
        for cert_doc in cert_docs:
            data          = cert_doc.to_dict()
            pdf_url       = data.get('pdfUrl', '')
            lecture_title = data.get('lectureTitle') or cert_doc.id
            user_uid      = cert_doc.reference.parent.parent.id

            if not pdf_url:
                continue

            found_any = True
            safe_title = re.sub(r'[^\w가-힣_-]', '_', lecture_title)
            entry_name = f"{user_uid}_{safe_title}.pdf"

            try:
                resp = requests.get(pdf_url, timeout=30)
                if resp.status_code == 200:
                    zf.writestr(entry_name, resp.content)
                else:
                    app.logger.warning(f"PDF 다운로드 실패 ({resp.status_code}): {pdf_url}")
            except Exception as fetch_ex:
                app.logger.error(f"PDF 다운로드 오류: {pdf_url} -> {fetch_ex}")

        if not found_any:
            zf.close()
            try:
                os.remove(tmp_zip_path)
            except OSError:
                pass
            return abort(404, description=f"{week_param}에 발급된 수료증이 없습니다.")

    try:
        s3.upload_file(
            Filename=tmp_zip_path,
            Bucket=BUCKET_NAME,
            Key=zip_key,
            Config=config
        )
    except Exception as upload_ex:
        app.logger.error(f"ZIP 업로드 실패: {upload_ex}")
        return abort(500, description="ZIP 업로드 중 오류가 발생했습니다.")

    try:
        os.remove(tmp_zip_path)
    except OSError:
        pass

    try:
        presigned = generate_presigned_url(zip_key, expires_in=3600)
    except Exception as pre_ex:
        app.logger.error(f"Presigned URL 생성 실패: {pre_ex}")
        return abort(500, description="Presigned URL 생성 중 오류가 발생했습니다.")

    return jsonify({
        'zipUrl': presigned,
        'generated': True,
        'week': week_param
    })

@app.route('/api/admin/users/certs/zip', methods=['GET'])
@admin_required
def generate_selected_zip():
    """
    Flutter 호출용: 선택한 UID의 수료증 ZIP 생성/조회
    - query param: uids=uid1,uid2,...  (콤마로 구분된 UID 목록)
                   type=recent|all      ('recent': 이번 주차만, 'all': 전체)
    """
    uids_param = request.args.get('uids')
    type_param = request.args.get('type')
    if not uids_param or type_param not in ('recent', 'all'):
        return abort(400, "uids와 type(recent 또는 all) 파라미터가 필요합니다.")

    uid_list = uids_param.split(',')

    if type_param == 'recent':
        today = datetime.utcnow().date()
        y, w, _ = today.isocalendar()
        week_str = f"{y}-W{str(w).zfill(2)}"
        try:
            week_start_dt, week_end_dt = parse_iso_week(week_str)
        except ValueError as ex:
            return abort(400, description=str(ex))

        start_ts = firestore.Timestamp.from_datetime(week_start_dt)
        end_ts   = firestore.Timestamp.from_datetime(week_end_dt)

    tmp_zip_path = f"/tmp/selected_{uuid.uuid4().hex}.zip"
    with zipfile.ZipFile(tmp_zip_path, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        found_any = False

        for uid in uid_list:
            coll_ref = db.collection('users').document(uid).collection('completedCertificates')
            if type_param == 'recent':
                docs = coll_ref \
                    .where('issuedAt', '>=', start_ts) \
                    .where('issuedAt', '<=', end_ts) \
                    .stream()
            else:  # 'all'
                docs = coll_ref.stream()

            for cert_doc in docs:
                data          = cert_doc.to_dict()
                pdf_url       = data.get('pdfUrl', '')
                lecture_title = data.get('lectureTitle') or cert_doc.id

                if not pdf_url:
                    continue

                found_any = True
                safe_title = re.sub(r'[^\w가-힣_-]', '_', lecture_title)
                entry_name = f"{uid}_{safe_title}.pdf"

                try:
                    resp = requests.get(pdf_url, timeout=30)
                    if resp.status_code == 200:
                        zf.writestr(entry_name, resp.content)
                    else:
                        app.logger.warning(f"PDF 다운로드 실패 ({resp.status_code}): {pdf_url}")
                except Exception as fetch_ex:
                    app.logger.error(f"PDF 다운로드 오류: {pdf_url} -> {fetch_ex}")

        if not found_any:
            zf.close()
            try:
                os.remove(tmp_zip_path)
            except OSError:
                pass
            return abort(404, description="선택된 사용자의 수료증이 없습니다.")

    zip_key = f"selected/{uuid.uuid4().hex}.zip"
    try:
        s3.upload_file(
            Filename=tmp_zip_path,
            Bucket=BUCKET_NAME,
            Key=zip_key,
            Config=config
        )
    except Exception as upload_ex:
        app.logger.error(f"ZIP 업로드 실패: {upload_ex}")
        return abort(500, description="ZIP 업로드 중 오류가 발생했습니다.")

    try:
        os.remove(tmp_zip_path)
    except OSError:
        pass

    try:
        presigned = generate_presigned_url(zip_key, expires_in=3600)
    except Exception as pre_ex:
        app.logger.error(f"Presigned URL 생성 실패: {pre_ex}")
        return abort(500, description="Presigned URL 생성 중 오류가 발생했습니다.")

    return jsonify({
        'zipUrl': presigned,
        'generated': True
    })

# ===================================================================
# 앱 시작 시 스케줄러 자동 실행
# ===================================================================

if __name__ == "__main__":
    # 스케줄러 시작
    start_background_scheduler()
    
    port = int(os.environ.get("PORT", 8080))
    # 운영 모드로 실행 (디버거 비활성화)
    app.run(host="0.0.0.0", port=port, debug=False)