import os
import uuid
import re
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, date
from flask import (
    Flask,
    request,
    render_template,
    redirect,
    url_for,
    session,
    abort,
    jsonify
)
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image
from urllib.parse import urlparse, parse_qs
import requests
import zipfile

import firebase_admin
from firebase_admin import credentials, firestore, storage

# ==== 환경변수 설정 ====
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', 'changeme')
AWS_ACCESS_KEY   = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY   = os.environ['AWS_SECRET_KEY']
REGION_NAME      = os.environ['REGION_NAME']
BUCKET_NAME      = os.environ['BUCKET_NAME']
APP_BASE_URL     = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
SECRET_KEY       = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

# ==== Firebase Admin + Firestore 초기화 ====
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
    # Firebase Storage를 사용하려면 initialize_app에 storageBucket 지정
    firebase_admin.initialize_app(cred, {
        'storageBucket': f"{os.environ['project_id']}.appspot.com"
    })

db = firestore.client()
bucket = storage.bucket()

# ==== Flask 앱 설정 ====
app = Flask(__name__)
app.secret_key                   = SECRET_KEY
app.config['UPLOAD_FOLDER']      = 'static'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB
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
def generate_presigned_url(key: str, expires_in: int = 86400) -> str:
    """S3 객체에 대해 presigned URL 생성 (만료: expires_in 초)"""
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def create_qr_with_logo(link_url: str, output_path: str,
                        logo_path: str = 'static/logo.png',
                        size_ratio: float = 0.25) -> None:
    """QR 코드 생성 후 중앙에 로고 삽입"""
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

def is_presigned_url_expired(url: str, safety_margin_minutes: int = 60) -> bool:
    """presigned URL 만료 여부 확인 (만료까지 safety_margin_minutes 이전부터 만료로 간주)"""
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
    except Exception:
        return True

def parse_iso_week(week_str: str):
    """
    week_str 형식: "YYYY-Www" (예: "2025-W23")
    → 해당 ISO 주의 월요일 00:00:00 ~ 일요일 23:59:59 (UTC) 반환
    """
    try:
        year_part, week_part = week_str.split('-W')
        year = int(year_part)
        week_num = int(week_part)
        week_start_date = date.fromisocalendar(year, week_num, 1)  # 월요일
        week_end_date = week_start_date + timedelta(days=6)        # 일요일

        week_start_dt = datetime.combine(week_start_date, datetime.min.time())
        week_end_dt   = datetime.combine(week_end_date, datetime.max.time())
        return week_start_dt, week_end_dt
    except Exception as e:
        raise ValueError(f"잘못된 week_str 형식: {week_str} ({e})")


# ==== 라우팅 설정 ====

@app.route('/', methods=['GET'])
def login_page():
    """관리자 로그인 페이지 렌더링"""
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    """관리자 비밀번호 검증 후 세션 부여"""
    pw = request.form.get('password', '')
    if pw == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('upload_form'))
    return render_template('login.html', error="비밀번호가 올바르지 않습니다.")

@app.route('/upload_form', methods=['GET'])
def upload_form():
    """동영상 업로드 폼 (인증 필요)"""
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

@app.route('/upload', methods=['POST'])
def upload_video():
    """동영상 업로드 처리 → S3 업로드, Firestore에 메타데이터 저장"""
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file          = request.files.get('file')
    group_name    = request.form.get('group_name', 'default')
    main_cat      = request.form.get('main_category', '')
    sub_cat       = request.form.get('sub_category', '')
    leaf_cat      = request.form.get('sub_sub_category', '')
    lecture_time  = request.form.get('time', '')
    lecture_level = request.form.get('level', '')
    lecture_tag   = request.form.get('tag', '')

    if not file:
        return "파일이 필요합니다.", 400

    # 1) 그룹 ID 생성 및 S3 키 구성
    group_id  = uuid.uuid4().hex
    date_str  = datetime.utcnow().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder    = f"videos/{group_id}_{safe_name}_{date_str}"
    ext       = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"

    # 2) 임시 저장 후 S3 업로드
    tmp_path = Path(tempfile.gettempdir()) / f"{group_id}{ext}"
    file.save(tmp_path)
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    # 3) Presigned URL 생성 (2주일 유효)
    presigned_url = generate_presigned_url(video_key, expires_in=604800)

    # 4) QR 링크 (앱 Watch URL)
    qr_link = f"{APP_BASE_URL}{group_id}"

    # 5) QR 코드 생성 및 S3 업로드
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(qr_link, local_qr)
    qr_key      = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    # 6) Firestore에 메타데이터 저장
    db.collection('uploads').document(group_id).set({
        'group_id':         group_id,
        'group_name':       group_name,
        'main_category':    main_cat,
        'sub_category':     sub_cat,
        'sub_sub_category': leaf_cat,
        'time':             lecture_time,
        'level':            lecture_level,
        'tag':              lecture_tag,
        'video_key':        video_key,
        'presigned_url':    presigned_url,
        'qr_link':          qr_link,
        'qr_key':           qr_key,
        'upload_date':      date_str
    })

    return render_template(
        'success.html',
        group_id      = group_id,
        main          = main_cat,
        sub           = sub_cat,
        leaf          = leaf_cat,
        time          = lecture_time,
        level         = lecture_level,
        tag           = lecture_tag,
        presigned_url = presigned_url,
        qr_link       = qr_link,
        qr_url        = url_for('static', filename=qr_filename)
    )

@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    """
    동영상 시청 페이지 (Presigned URL 갱신 포함)
    - Firestore의 presigned_url 만료 확인 후, 새로 발급하여 업데이트
    """
    doc_ref = db.collection('uploads').document(group_id)
    doc     = doc_ref.get()
    if not doc.exists:
        abort(404)
    data = doc.to_dict()

    current_presigned = data.get('presigned_url', '')
    if not current_presigned or is_presigned_url_expired(current_presigned, 60):
        # presigned URL이 없거나 곧 만료될 예정이면 재발급
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
def generate_weekly_zip():
    """
    관리자가 특정 주차 전체 수료증 ZIP 요청 (Firestore에서 인증된 관리자 이용)
    - query param: week (예: "2025-W23")
    """
    if not session.get('logged_in'):
        abort(401)  # 인증 필요

    week_param = request.args.get('week')
    if not week_param:
        today = datetime.utcnow().date()
        y, w, _ = today.isocalendar()
        week_param = f"{y}-W{str(w).zfill(2)}"

    zip_key = f"full/{week_param}.zip"

    # 1) 이미 S3(Wasabi)에 ZIP이 존재하는지 확인
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
            abort(500, description=f"S3 오류: {e}")

    # 2) ZIP이 없으면 Firestore에서 해당 주차 수료증 조회
    try:
        week_start_dt, week_end_dt = parse_iso_week(week_param)
    except ValueError as ex:
        abort(400, description=str(ex))

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
            data = cert_doc.to_dict()
            pdf_url = data.get('pdfUrl', '')
            lecture_title = data.get('lectureTitle') or cert_doc.id
            user_uid = cert_doc.reference.parent.parent.id  # users/{uid}/completedCertificates/{doc}

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
            except:
                pass
            abort(404, description=f"{week_param}에 발급된 수료증이 없습니다.")

    # 3) 완성된 ZIP을 Wasabi(S3)에 업로드
    try:
        s3.upload_file(
            Filename=tmp_zip_path,
            Bucket=BUCKET_NAME,
            Key=zip_key,
            Config=config
        )
    except Exception as upload_ex:
        app.logger.error(f"ZIP 업로드 실패: {upload_ex}")
        abort(500, description="ZIP 업로드 중 오류가 발생했습니다.")

    # 임시 ZIP 삭제
    try:
        os.remove(tmp_zip_path)
    except:
        pass

    # 4) presigned URL 생성 및 반환
    try:
        presigned = generate_presigned_url(zip_key, expires_in=3600)
    except Exception as pre_ex:
        app.logger.error(f"Presigned URL 생성 실패: {pre_ex}")
        abort(500, description="Presigned URL 생성 중 오류가 발생했습니다.")

    return jsonify({
        'zipUrl': presigned,
        'generated': True,
        'week': week_param
    })

@app.route('/api/admin/users/certs/zip', methods=['GET'])
def generate_selected_zip():
    """
    Flutter에서 호출하는 엔드포인트:
    - query param: uids=uid1,uid2,...  (콤마로 구분된 UID 목록)
                   type=recent|all    ('recent'이면 이번 주차, 'all'이면 전체)
    - 세션 기반 인증 (관리자만)
    """
    if not session.get('logged_in'):
        abort(401)

    uids_param = request.args.get('uids')
    type_param = request.args.get('type')
    if not uids_param or type_param not in ('recent', 'all'):
        abort(400, "uids 및 type(recent 또는 all) 파라미터가 필요합니다.")

    uid_list = [u.strip() for u in uids_param.split(',') if u.strip()]
    if not uid_list:
        abort(400, "유효한 UID 목록이 없습니다.")

    # 'recent'인 경우, 이번 주차 범위를 계산
    if type_param == 'recent':
        today = datetime.utcnow().date()
        y, w, _ = today.isocalendar()
        week_str = f"{y}-W{str(w).zfill(2)}"
        try:
            week_start_dt, week_end_dt = parse_iso_week(week_str)
        except ValueError as ex:
            abort(400, description=str(ex))
        start_ts = firestore.Timestamp.from_datetime(week_start_dt)
        end_ts   = firestore.Timestamp.from_datetime(week_end_dt)

    # ZIP 파일 임시 경로
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
                data = cert_doc.to_dict()
                pdf_url = data.get('pdfUrl', '')
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
            except:
                pass
            abort(404, description="선택된 사용자의 수료증이 없습니다.")

    # 6) 생성된 ZIP을 Wasabi(S3)에 업로드
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
        abort(500, description="ZIP 업로드 중 오류가 발생했습니다.")

    # 임시 ZIP 삭제
    try:
        os.remove(tmp_zip_path)
    except:
        pass

    # 7) 업로드된 ZIP의 presigned URL 생성
    try:
        presigned = generate_presigned_url(zip_key, expires_in=3600)
    except Exception as pre_ex:
        app.logger.error(f"Presigned URL 생성 실패: {pre_ex}")
        abort(500, description="Presigned URL 생성 중 오류가 발생했습니다.")

    return jsonify({
        'zipUrl': presigned,
        'generated': True
    })

# ==== 서버 실행 ====
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
