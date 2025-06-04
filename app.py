import os
import uuid
import re
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

import firebase_admin
from firebase_admin import credentials, firestore

# ==== 환경변수 설정 ====
# 관리자 비밀번호 (기본: changeme)
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', 'changeme')

# Wasabi S3 연동용 키
AWS_ACCESS_KEY   = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY   = os.environ['AWS_SECRET_KEY']
REGION_NAME      = os.environ['REGION_NAME']
BUCKET_NAME      = os.environ['BUCKET_NAME']

# Branch 딥링크용 키 (v2 REST API)
BRANCH_KEY       = os.environ.get('BRANCH_KEY', '')

# 웹 fallback을 위해 쓰일 기본 URL
APP_BASE_URL     = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')

# Flask 세션용 시크릿 키
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
    firebase_admin.initialize_app(cred)

db = firestore.client()


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

def generate_presigned_url(key, expires_in=86400):
    """
    S3 객체에 대해 presigned URL 생성
    expires_in: URL 유효 기간(초 단위)
    """
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )


def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', size_ratio=0.25):
    """
    QR 코드 생성 후 중앙에 로고 삽입
    - link_url: QR에 인코딩할 URL (Branch 딥링크 혹은 웹 URL)
    - output_path: 저장할 로컬 경로 (예: 'static/abcd1234.png')
    - logo_path: QR 중앙에 삽입할 로고 (optional)
    - size_ratio: 로고가 QR 전체 대비 차지할 비율
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
    - url: presigned URL
    - safety_margin_minutes: 안전 여유 시간(분) 이후에도 만료되었는지 체크
    """
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


def create_branch_deep_link(group_id: str) -> str:
    """
    Branch REST API를 호출해서 딥링크(URL)를 생성한다.
    - group_id: Firestore 문서 ID이자 영상 식별자
    - 반환값: Branch가 발급한 딥링크 URL
    """
    if not BRANCH_KEY:
        raise RuntimeError("환경변수 BRANCH_KEY가 설정되지 않았습니다.")

    ios_uri      = f"myapp://watch/{group_id}"
    android_uri  = f"myapp://watch/{group_id}"
    web_fallback = f"{APP_BASE_URL}{group_id}"

    payload = {
        "branch_key": BRANCH_KEY,
        "campaign":   "VideoShare",
        "channel":    "QR",
        "data": {
            "group_id":      group_id,
            "$desktop_url":  web_fallback,
            "$ios_url":      ios_uri,
            "$android_url":  android_uri,
            "$fallback_url": web_fallback
        }
    }

    resp = requests.post(
        "https://api2.branch.io/v1/url",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10
    )
    resp.raise_for_status()
    result = resp.json()
    return result.get("url", "")


def download_file_to_bytes(url: str) -> bytes:
    """
    주어진 URL로부터 파일(주로 PDF)을 HTTP GET 요청해서 바이트로 반환
    """
    resp = requests.get(url, stream=True, timeout=20)
    resp.raise_for_status()
    return resp.content


def build_user_certs_zip(uid: str) -> BytesIO:
    """
    1) Firestore에서 users/{uid}/completedCertificates 서브컬렉션 조회
    2) 각 문서의 'pdfUrl'을 모아서, 모두 다운로드 후 ZIP으로 압축해서 메모리(BytesIO)에 담아 리턴
    """
    from io import BytesIO

    docs = (
        db.collection("users")
        .document(uid)
        .collection("completedCertificates")
        .order_by("issuedAt", direction=firestore.Query.ASCENDING)
        .stream()
    )

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for idx, doc in enumerate(docs):
            data = doc.to_dict()
            pdf_url = data.get("pdfUrl", "")
            lecture_title = data.get("lectureTitle", f"certificate_{idx}")
            if not pdf_url:
                continue

            try:
                file_bytes = download_file_to_bytes(pdf_url)
            except Exception as e:
                print(f"PDF 다운로드 실패 (url={pdf_url}): {e}")
                continue

            safe_name = lecture_title.replace(" ", "_").replace("/", "_")
            filename = f"{safe_name}_{idx+1}.pdf"
            zipf.writestr(filename, file_bytes)

    zip_buffer.seek(0)
    return zip_buffer


# ==== 라우팅 설정 ====

@app.route('/', methods=['GET'])
def login_page():
    """로그인 페이지 렌더링"""
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
    """
    업로드 폼 페이지 (인증 필요)
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


@app.route('/upload', methods=['POST'])
def upload_video():
    """
    동영상 업로드 처리 + Branch 딥링크 생성 + QR 코드 생성
    """
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
    date_str  = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder    = f"videos/{group_id}_{safe_name}_{date_str}"
    ext       = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"

    # 2) 임시 저장 및 S3 업로드
    tmp_path = Path(tempfile.gettempdir()) / f"{group_id}{ext}"
    file.save(tmp_path)
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    # 3) Presigned URL 생성 (2주일 유효, 604800초)
    presigned_url = generate_presigned_url(video_key, expires_in=604800)

    # 4) Branch 딥링크 생성 → QR 링크에 넣음
    try:
        qr_link = create_branch_deep_link(group_id)
    except Exception as e:
        app.logger.error(f"Branch 딥링크 생성 실패: {e}")
        # Branch API 호출 실패 시 fallback으로 웹 URL만 사용
        qr_link = f"{APP_BASE_URL}{group_id}"

    # 5) QR 생성 및 S3 업로드
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr    = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    create_qr_with_logo(qr_link, local_qr)
    qr_key      = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)

    # 6) Firestore 메타데이터 저장 (branch_link 필드도 추가)
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
        'branch_link':      qr_link,
        'qr_key':           qr_key,
        'upload_date':      date_str
    })

    # 7) 업로드 성공 페이지 렌더링 (branch_link, qr_url 등 전달)
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
        branch_link   = qr_link,
        qr_url        = url_for('static', filename=qr_filename)
    )


@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    """
    동영상 시청 페이지 (Presigned URL 갱신 포함)
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


@app.route('/api/admin/users/<uid>/certs/zip', methods=['GET'])
def get_user_certs_zip(uid):
    """
    사용자의 전체 수료증을 하나의 ZIP으로 묶어서 Wasabi에 저장하고
    presigned URL을 반환하거나, 이미 저장된 파일이 있으면 URL만 반환
    """
    # (1) 인증/권한 검사 (관리자 전용) 생략 - 실제 서비스에 맞게 구현 필요
    # token = request.headers.get("Authorization", "").replace("Bearer ", "")
    # if not is_admin(token): return abort(403)

    # (2) S3에 이미 저장된 ZIP 키 구성
    zip_key = f"certs/certs_full_{uid}.zip"

    # (3) S3에 해당 키가 이미 존재하는지 확인
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=zip_key)
        url = generate_presigned_url(zip_key, expires_in=3600 * 24)
        return jsonify({"url": url, "cached": True})
    except s3.exceptions.ClientError as e:
        # 객체가 없는 경우 404 응답 → 새로 생성해야 함
        if e.response['Error']['Code'] not in ('404', 'NoSuchKey'):
            abort(500, description=f"S3 오류: {e}")

    # (4) ZIP이 없으면 새로 생성
    try:
        from io import BytesIO
        # 4-1) 사용자의 모든 수료증 PDF를 다운로드 후 ZIP으로 묶기
        zip_buffer = build_user_certs_zip(uid)

        # 4-2) Wasabi(S3)에 업로드
        s3.upload_fileobj(
            zip_buffer,
            BUCKET_NAME,
            zip_key,
            ExtraArgs={'ContentType': 'application/zip'}
        )

        # 4-3) Presigned URL 생성
        url = generate_presigned_url(zip_key, expires_in=3600 * 24)
        return jsonify({"url": url, "cached": False})
    except Exception as ex:
        print("ZIP 생성/업로드 중 오류:", ex)
        abort(500, description="ZIP 생성 중 오류가 발생했습니다.")


@app.route('/generate_weekly_zip', methods=['GET'])
def generate_weekly_zip():
    """
    관리자가 특정 주차 전체 수료증 ZIP을 요청할 때 호출합니다.
    - query param: week (예: "2025-W23")
    - 세션 검사: 로그인된 관리자 세션만 허용
    """
    if not session.get('logged_in'):
        abort(401)

    week_param = request.args.get('week')
    if not week_param:
        today = datetime.utcnow().date()
        y, w, _ = today.isocalendar()
        week_param = f"{y}-W{str(w).zfill(2)}"

    zip_key = f"full/{week_param}.zip"

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=zip_key)
        presigned = generate_presigned_url(zip_key, expires_in=3600)
        return {
            'zipUrl': presigned,
            'generated': False,
            'week': week_param
        }
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            abort(500, description=f"S3 오류: {e}")

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
            user_uid = cert_doc.reference.parent.parent.id

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

    try:
        os.remove(tmp_zip_path)
    except:
        pass

    try:
        presigned = generate_presigned_url(zip_key, expires_in=3600)
    except Exception as pre_ex:
        app.logger.error(f"Presigned URL 생성 실패: {pre_ex}")
        abort(500, description="Presigned URL 생성 중 오류가 발생했습니다.")

    return {
        'zipUrl': presigned,
        'generated': True,
        'week': week_param
    }


# ==== 서버 실행 ====
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
