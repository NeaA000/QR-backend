#!/usr/bin/env python3
"""
통합 Flask 백엔드 애플리케이션
Railway 빌드 속도 최적화를 위해 하나의 파일로 통합
각 섹션별 기능 설명 주석 포함
"""

import os
import re
import uuid
import time
import json
import hashlib
import tempfile
import qrcode
import logging
import urllib.request
import atexit
import jwt
import boto3
import firebase_admin
import pandas as pd
import io
import threading

from pathlib import Path
from datetime import datetime, timedelta, date
from functools import wraps, lru_cache
from typing import Dict, List, Optional, Tuple, Any

from flask import Flask, request, jsonify, render_template, redirect, url_for, session, abort, make_response, g
from werkzeug.utils import secure_filename

from PIL import Image, ImageDraw, ImageFont
from moviepy.editor import VideoFileClip
from googletrans import Translator
from firebase_admin import credentials, firestore, storage as firebase_storage
from boto3.s3.transfer import TransferConfig
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from urllib.parse import urlparse, parse_qs

# ===========================
# 로깅 설정
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===========================
# 환경 변수 및 설정
# ===========================
# Flask 설정
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'supersecret')
UPLOAD_FOLDER = 'static'
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB

# 관리자 인증 설정
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'changeme')
JWT_SECRET = os.environ.get('JWT_SECRET', 'supersecretjwt')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRES_HOURS = 4

# AWS S3 설정 (Wasabi)
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
REGION_NAME = os.environ['REGION_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']

# Firebase 설정
FIREBASE_CREDS = {
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

# 기타 설정
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
SUPPORTED_LANGUAGES = {
    'ko': '한국어',
    'en': 'English',
    'zh-cn': '中文',
    'vi': 'Tiếng Việt',
    'th': 'ไทย',
    'uz': 'O\'zbek',
    'ja': '日本語'
}
ALLOWED_VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm', '.m4v', '.mpg', '.mpeg'}
ALLOWED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}

# 디렉토리 생성
Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path("fonts").mkdir(exist_ok=True)

# ===========================
# S3 클라이언트 설정
# ===========================
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=f'https://s3.{REGION_NAME}.wasabisys.com'
)

s3_config = TransferConfig(
    multipart_threshold=1024 * 1024 * 25,  # 25MB 이상 멀티파트
    multipart_chunksize=1024 * 1024 * 50,  # 50MB 청크
    max_concurrency=5,  # 동시 업로드 수
    use_threads=True  # 스레드 사용
)

# ===========================
# Firebase 초기화
# ===========================
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_CREDS)
    firebase_admin.initialize_app(cred, {
        'storageBucket': f"{FIREBASE_CREDS['project_id']}.appspot.com"
    })

firebase_bucket = firebase_storage.bucket()
db = firestore.client()

# ===========================
# 번역기 초기화
# ===========================
translator = Translator()

# ===========================
# Flask 앱 생성
# ===========================
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# ===========================
# S3 관련 함수
# ===========================
def generate_presigned_url(key: str, expires_in: int = 86400) -> str:
    """S3 객체에 대한 Presigned URL 생성"""
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def upload_to_s3(file_path: str, key: str) -> None:
    """파일을 S3에 업로드"""
    s3.upload_file(str(file_path), BUCKET_NAME, key, Config=s3_config)

def upload_to_firebase(file_path: str, blob_name: str) -> str:
    """파일을 Firebase Storage에 업로드"""
    blob = firebase_bucket.blob(blob_name)
    blob.upload_from_filename(str(file_path))
    return blob.public_url

# ===========================
# Database 함수
# ===========================
def get_video_document(group_id: str) -> Optional[Dict]:
    """비디오 문서 조회"""
    doc = db.collection('uploads').document(group_id).get()
    return doc.to_dict() if doc.exists else None

def update_video_document(group_id: str, data: Dict) -> None:
    """비디오 문서 업데이트"""
    db.collection('uploads').document(group_id).update(data)

def create_video_document(group_id: str, data: Dict) -> None:
    """비디오 문서 생성"""
    db.collection('uploads').document(group_id).set(data)

def get_translation(group_id: str, lang_code: str) -> Optional[Dict]:
    """번역 조회"""
    doc = db.collection('uploads').document(group_id) \
           .collection('translations').document(lang_code).get()
    return doc.to_dict() if doc.exists else None

def save_translation(group_id: str, lang_code: str, data: Dict) -> None:
    """번역 저장"""
    db.collection('uploads').document(group_id) \
      .collection('translations').document(lang_code) \
      .set(data)

# ===========================
# 번역 관련 함수
# ===========================
def translate_text(text: str, target_language: str) -> str:
    """텍스트를 지정된 언어로 번역"""
    try:
        if target_language == 'ko' or not text.strip():
            return text
        
        # 언어 코드 변환 (zh-cn -> zh)
        google_lang_code = target_language
        if target_language == 'zh-cn':
            google_lang_code = 'zh'
        
        result = translator.translate(text, src='ko', dest=google_lang_code)
        translated_text = result.text
        
        logger.info(f"번역 완료: '{text}' → '{translated_text}' ({target_language})")
        return translated_text
        
    except Exception as e:
        logger.warning(f"번역 실패 ({target_language}): {e}, 원본 텍스트 사용")
        return text

def create_multilingual_metadata(korean_text: str) -> Dict[str, str]:
    """한국어 텍스트를 모든 지원 언어로 번역"""
    translations = {}
    
    if not korean_text.strip():
        return {lang: '' for lang in SUPPORTED_LANGUAGES.keys()}
    
    for lang_code in SUPPORTED_LANGUAGES.keys():
        try:
            translated = translate_text(korean_text, lang_code)
            translations[lang_code] = translated
            
            # API 제한 회피를 위한 지연
            if lang_code != 'ko':
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"언어 {lang_code} 번역 중 오류: {e}")
            translations[lang_code] = korean_text
    
    return translations

# ===========================
# QR 코드 생성 관련
# ===========================
# 한국어 폰트 URL 목록
KOREAN_FONT_URLS = [
    "https://cdn.jsdelivr.net/gh/google/fonts/ofl/notosanskr/NotoSansKR-Regular.ttf",
    "https://fonts.gstatic.com/ea/notosanskr/v2/NotoSansKR-Regular.otf",
    "https://github.com/notofonts/noto-cjk/raw/main/Sans/OTF/Korean/NotoSansCJKkr-Regular.otf",
    "https://raw.githubusercontent.com/google/fonts/main/ofl/notosanskr/NotoSansKR-Regular.ttf"
]

def download_korean_font() -> Optional[str]:
    """한국어 폰트 다운로드"""
    font_dir = Path("fonts")
    font_dir.mkdir(exist_ok=True)
    
    font_path = font_dir / "NotoSansKR-Regular.ttf"
    
    # 이미 다운로드된 경우
    if font_path.exists() and font_path.stat().st_size > 100000:
        return str(font_path)
    
    # 폰트 다운로드 시도
    for i, font_url in enumerate(KOREAN_FONT_URLS):
        try:
            logger.info(f"📥 한국어 폰트 다운로드 시도 {i+1}/{len(KOREAN_FONT_URLS)}: {font_url}")
            urllib.request.urlretrieve(font_url, font_path)
            
            if font_path.exists() and font_path.stat().st_size > 100000:
                logger.info(f"✅ 폰트 다운로드 완료: {font_path}")
                return str(font_path)
            else:
                font_path.unlink(missing_ok=True)
                
        except Exception as e:
            logger.warning(f"❌ 폰트 다운로드 실패 ({i+1}): {e}")
            font_path.unlink(missing_ok=True)
    
    logger.error("❌ 모든 폰트 다운로드 시도 실패")
    return None

def get_korean_font(size: int = 32) -> ImageFont.FreeTypeFont:
    """한국어 폰트 로드"""
    try:
        # 다운로드한 폰트 시도
        korean_font_path = download_korean_font()
        if korean_font_path and os.path.exists(korean_font_path):
            try:
                font = ImageFont.truetype(korean_font_path, size)
                logger.info(f"✅ 한국어 폰트 사용: {korean_font_path}")
                return font
            except Exception as e:
                logger.warning(f"폰트 로드 실패: {e}")
        
        # 시스템 폰트 시도
        system_fonts = [
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
            'C:\\Windows\\Fonts\\malgun.ttf',
            '/System/Library/Fonts/AppleSDGothicNeo.ttc',
        ]
        
        for font_path in system_fonts:
            if os.path.exists(font_path):
                try:
                    font = ImageFont.truetype(font_path, size)
                    logger.info(f"✅ 시스템 폰트 사용: {font_path}")
                    return font
                except Exception:
                    continue
        
        logger.warning("⚠️ 한국어 폰트를 찾을 수 없어 기본 폰트 사용")
        return ImageFont.load_default()
        
    except Exception as e:
        logger.error(f"폰트 로드 중 오류: {e}")
        return ImageFont.load_default()

def create_qr_with_logo(link_url: str, output_path: str, logo_path: str = 'static/logo.png', lecture_title: str = "") -> None:
    """로고와 텍스트가 포함된 QR 코드 생성"""
    try:
        # QR 코드 생성
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=15,
            border=4,
        )
        qr.add_data(link_url)
        qr.make(fit=True)
        
        # QR 이미지 생성
        qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGBA")
        qr_size = 600
        qr_img = qr_img.resize((qr_size, qr_size), Image.LANCZOS)
        
        # 중앙 공간을 흰색으로 채우기
        draw_qr = ImageDraw.Draw(qr_img)
        center_size = int(qr_size * 0.3)
        center_pos = (qr_size - center_size) // 2
        draw_qr.rectangle(
            [center_pos, center_pos, center_pos + center_size, center_pos + center_size],
            fill="white"
        )
        
        # 로고 삽입
        if os.path.exists(logo_path):
            try:
                logo = Image.open(logo_path).convert("RGBA")
                logo_size = int(qr_size * 0.25)
                logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
                logo_pos = ((qr_size - logo_size) // 2, (qr_size - logo_size) // 2)
                qr_img.paste(logo, logo_pos, logo)
            except Exception as e:
                logger.warning(f"로고 삽입 실패: {e}")
        
        # RGB로 변환
        final_img = Image.new('RGB', qr_img.size, 'white')
        final_img.paste(qr_img, (0, 0), qr_img if qr_img.mode == 'RGBA' else None)
        
        # 텍스트 추가
        if lecture_title.strip():
            text_height = 120
            margin = 20
            
            total_height = qr_size + text_height + margin
            final_with_text = Image.new('RGB', (qr_size, total_height), 'white')
            final_with_text.paste(final_img, (0, 0))
            
            draw = ImageDraw.Draw(final_with_text)
            font_size = 36
            font = get_korean_font(font_size)
            
            # 텍스트를 줄로 나누기
            max_width = qr_size - 40
            lines = []
            words = lecture_title.split()
            current_line = ""
            
            for word in words:
                test_line = current_line + " " + word if current_line else word
                try:
                    bbox = draw.textbbox((0, 0), test_line, font=font)
                    text_width = bbox[2] - bbox[0]
                except:
                    text_width = len(test_line) * (font_size * 0.7)
                
                if text_width <= max_width:
                    current_line = test_line
                else:
                    if current_line:
                        lines.append(current_line)
                    current_line = word
            
            if current_line:
                lines.append(current_line)
            
            # 최대 2줄로 제한
            if len(lines) > 2:
                lines = lines[:2]
                lines[1] = lines[1][:25] + "..."
            
            # 텍스트 그리기
            y_offset = qr_size + margin
            for line in lines:
                try:
                    bbox = draw.textbbox((0, 0), line, font=font)
                    text_width = bbox[2] - bbox[0]
                    text_x = (qr_size - text_width) // 2
                    
                    # 그림자 효과
                    shadow_offset = 2
                    draw.text((text_x + shadow_offset, y_offset + shadow_offset), 
                             line, font=font, fill='lightgray')
                    
                    # 메인 텍스트
                    draw.text((text_x, y_offset), line, font=font, fill='black')
                    
                    y_offset += font_size + 10
                except Exception as e:
                    logger.warning(f"텍스트 렌더링 오류: {e}")
                    draw.text((20, y_offset), line, fill='black')
                    y_offset += 30
            
            final_img = final_with_text
        
        # 저장
        final_img.save(output_path, quality=95, optimize=True)
        logger.info(f"✅ QR 코드 생성 완료: {output_path}")
        
    except Exception as e:
        logger.error(f"❌ QR 코드 생성 실패: {e}")
        simple_qr = qrcode.make(link_url)
        simple_qr.save(output_path)

# ===========================
# 비디오 처리 관련 함수
# ===========================
def get_video_duration(file_path: str) -> Tuple[str, int]:
    """비디오 파일의 재생 시간 추출"""
    try:
        with VideoFileClip(str(file_path)) as clip:
            duration_sec = int(clip.duration)
            minutes = duration_sec // 60
            seconds = duration_sec % 60
            return f"{minutes}:{seconds:02d}", duration_sec
    except Exception as e:
        logger.warning(f"비디오 길이 가져오기 실패: {e}")
        return "0:00", 0

def is_allowed_file(filename: str, allowed_extensions: set) -> bool:
    """파일 확장자 확인"""
    return Path(filename).suffix.lower() in allowed_extensions

def process_video_upload(file, group_name: str, main_cat: str, sub_cat: str, 
                        leaf_cat: str, level: str, tag: str, thumbnail_file=None) -> Dict:
    """비디오 업로드 전체 프로세스 처리"""
    try:
        # 파일 확장자 확인
        if not is_allowed_file(file.filename, ALLOWED_VIDEO_EXTENSIONS):
            raise ValueError(f"지원하지 않는 비디오 형식입니다. 지원 형식: {', '.join(ALLOWED_VIDEO_EXTENSIONS)}")
        
        # 다국어 번역
        logger.info(f"다국어 번역 시작: '{group_name}'")
        translated_titles = create_multilingual_metadata(group_name)
        translated_main_cat = create_multilingual_metadata(main_cat) if main_cat else {}
        translated_sub_cat = create_multilingual_metadata(sub_cat) if sub_cat else {}
        translated_leaf_cat = create_multilingual_metadata(leaf_cat) if leaf_cat else {}
        
        # 그룹 ID 및 경로 생성
        group_id = uuid.uuid4().hex
        date_str = datetime.now().strftime('%Y%m%d')
        safe_name = re.sub(r'[^\w가-힣]', '_', group_name)
        folder = f"videos/{group_id}_{safe_name}_{date_str}"
        
        # 비디오 저장 및 업로드
        ext = Path(file.filename).suffix.lower()
        video_key = f"{folder}/video{ext}"
        
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
            file.save(tmp_path)
        
        # 비디오 길이 측정
        lecture_time, duration_sec = get_video_duration(tmp_path)
        logger.info(f"비디오 길이: {lecture_time} (총 {duration_sec}초)")
        
        # S3 업로드
        upload_to_s3(tmp_path, video_key)
        tmp_path.unlink(missing_ok=True)
        
        # Presigned URL 생성
        presigned_url = generate_presigned_url(video_key, expires_in=604800)
        
        # 썸네일 처리
        thumbnail_url = ""
        thumbnail_key = ""
        if thumbnail_file and is_allowed_file(thumbnail_file.filename, ALLOWED_IMAGE_EXTENSIONS):
            try:
                thumb_ext = Path(thumbnail_file.filename).suffix.lower()
                thumbnail_key = f"{folder}/thumbnail{thumb_ext}"
                
                with tempfile.NamedTemporaryFile(suffix=thumb_ext, delete=False) as thumb_tmp:
                    thumb_path = Path(thumb_tmp.name)
                    thumbnail_file.save(thumb_path)
                
                # S3에 썸네일 업로드
                upload_to_s3(thumb_path, thumbnail_key)
                thumb_path.unlink(missing_ok=True)
                
                # 썸네일 URL 생성
                thumbnail_url = generate_presigned_url(thumbnail_key, expires_in=604800)
                logger.info(f"썸네일 업로드 완료: {thumbnail_key}")
                
            except Exception as e:
                logger.error(f"썸네일 처리 실패: {e}")
        
        # QR 코드 생성
        qr_link = f"{APP_BASE_URL}{group_id}"
        qr_filename = f"{uuid.uuid4().hex}.png"
        local_qr = os.path.join('static', qr_filename)
        
        # QR 코드에 표시할 텍스트
        display_title = group_name
        if main_cat or sub_cat or leaf_cat:
            categories = [cat for cat in [main_cat, sub_cat, leaf_cat] if cat]
            if categories:
                display_title = f"{group_name}\n({' > '.join(categories)})"
        
        create_qr_with_logo(qr_link, local_qr, lecture_title=display_title)
        
        # QR 코드 S3 업로드
        qr_key = f"{folder}/{qr_filename}"
        upload_to_s3(local_qr, qr_key)
        os.remove(local_qr)
        
        qr_presigned_url = generate_presigned_url(qr_key, expires_in=604800)
        
        # Firestore 문서 생성
        root_doc_data = {
            'group_id': group_id,
            'group_name': group_name,
            'main_category': main_cat,
            'sub_category': sub_cat,
            'sub_sub_category': leaf_cat,
            'time': lecture_time,
            'duration_seconds': duration_sec,
            'level': level,
            'tag': tag,
            'video_key': video_key,
            'presigned_url': presigned_url,
            'thumbnail_key': thumbnail_key,
            'thumbnail_url': thumbnail_url,
            'qr_link': qr_link,
            'qr_key': qr_key,
            'qr_presigned_url': qr_presigned_url,
            'upload_date': date_str,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        create_video_document(group_id, root_doc_data)
        
        # 번역 서브컬렉션 저장
        for lang_code in SUPPORTED_LANGUAGES.keys():
            if lang_code == 'ko':
                translation_data = {
                    'title': group_name,
                    'main_category': main_cat,
                    'sub_category': sub_cat,
                    'sub_sub_category': leaf_cat,
                    'language_code': lang_code,
                    'language_name': SUPPORTED_LANGUAGES[lang_code],
                    'is_original': True,
                    'translated_at': datetime.utcnow().isoformat()
                }
            else:
                translation_data = {
                    'title': translated_titles.get(lang_code, group_name),
                    'main_category': translated_main_cat.get(lang_code, main_cat),
                    'sub_category': translated_sub_cat.get(lang_code, sub_cat),
                    'sub_sub_category': translated_leaf_cat.get(lang_code, leaf_cat),
                    'language_code': lang_code,
                    'language_name': SUPPORTED_LANGUAGES[lang_code],
                    'is_original': False,
                    'translated_at': datetime.utcnow().isoformat()
                }
            
            save_translation(group_id, lang_code, translation_data)
        
        logger.info(f"✅ 비디오 업로드 완료: {group_id}")
        
        return {
            'success': True,
            'group_id': group_id,
            'translations': translated_titles,
            'time': lecture_time,
            'level': level,
            'tag': tag,
            'presigned_url': presigned_url,
            'thumbnail_url': thumbnail_url,
            'qr_url': qr_presigned_url
        }
        
    except Exception as e:
        logger.error(f"비디오 업로드 실패: {e}")
        raise

# ===========================
# 인증 관련 함수
# ===========================
def create_jwt_for_admin() -> str:
    """관리자용 JWT 토큰 생성"""
    now = datetime.utcnow()
    payload = {
        'sub': ADMIN_EMAIL,
        'iat': now,
        'exp': now + timedelta(hours=JWT_EXPIRES_HOURS)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token

def verify_jwt_token(token: str) -> bool:
    """JWT 토큰 검증"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get('sub') == ADMIN_EMAIL
    except jwt.ExpiredSignatureError:
        return False
    except Exception:
        return False

def admin_required(f):
    """관리자 인증 데코레이터 (API용)"""
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

def session_required(f):
    """세션 인증 데코레이터 (웹용)"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated

# ===========================
# 스케줄러 관련
# ===========================
scheduler = BackgroundScheduler(
    timezone='UTC',
    job_defaults={
        'coalesce': True,
        'max_instances': 1
    }
)

def is_presigned_url_expired(url: str, safety_margin_minutes: int = 60) -> bool:
    """Presigned URL이 만료되었는지 확인"""
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
        logger.warning(f"URL 검사 중 오류: {e}")
        return True

def refresh_expiring_urls():
    """만료 임박한 URL들을 자동으로 갱신"""
    try:
        logger.info("🔄 백그라운드 URL 갱신 작업 시작...")
        
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
                continue
            
            # URL 갱신 필요 확인
            if not current_url or is_presigned_url_expired(current_url, safety_margin_minutes=120):
                try:
                    # 새 URL 생성
                    new_presigned_url = generate_presigned_url(video_key, expires_in=604800)
                    
                    update_data = {
                        'presigned_url': new_presigned_url,
                        'auto_updated_at': datetime.utcnow().isoformat(),
                        'auto_update_reason': 'background_refresh'
                    }
                    
                    # QR URL도 갱신
                    qr_key = data.get('qr_key', '')
                    if qr_key:
                        new_qr_url = generate_presigned_url(qr_key, expires_in=604800)
                        update_data['qr_presigned_url'] = new_qr_url
                    
                    # 썸네일 URL 갱신
                    thumbnail_key = data.get('thumbnail_key', '')
                    if thumbnail_key:
                        new_thumbnail_url = generate_presigned_url(thumbnail_key, expires_in=604800)
                        update_data['thumbnail_url'] = new_thumbnail_url
                    
                    doc.reference.update(update_data)
                    updated_count += 1
                    logger.info(f"✅ 문서 {doc.id} URL 갱신 완료")
                    
                except Exception as e:
                    logger.error(f"❌ 문서 {doc.id} URL 갱신 실패: {e}")
        
        logger.info(f"🎉 URL 갱신 완료: {updated_count}/{total_count} 개")
        
    except Exception as e:
        logger.error(f"❌ URL 갱신 작업 중 오류: {e}")

def start_scheduler():
    """백그라운드 스케줄러 시작"""
    try:
        # URL 갱신 작업 (3시간마다)
        scheduler.add_job(
            func=refresh_expiring_urls,
            trigger=IntervalTrigger(hours=3),
            id='refresh_urls',
            name='URL 자동 갱신',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("🚀 백그라운드 스케줄러가 시작되었습니다.")
        
        # 앱 종료 시 스케줄러도 함께 종료
        atexit.register(lambda: scheduler.shutdown())
        
    except Exception as e:
        logger.error(f"❌ 스케줄러 시작 실패: {e}")

# ===========================
# 유틸리티 함수
# ===========================
def get_video_with_translation(group_id: str, lang_code: str = 'ko') -> Optional[Dict]:
    """특정 언어로 번역된 비디오 정보 조회"""
    try:
        # 루트 문서 조회
        root_doc = db.collection('uploads').document(group_id).get()
        if not root_doc.exists:
            return None
        
        root_data = root_doc.to_dict()
        
        # 번역 조회
        translation_data = get_translation(group_id, lang_code)
        
        if translation_data:
            root_data.update({
                'display_title': translation_data.get('title', root_data.get('group_name')),
                'display_main_category': translation_data.get('main_category', root_data.get('main_category')),
                'display_sub_category': translation_data.get('sub_category', root_data.get('sub_category')),
                'display_sub_sub_category': translation_data.get('sub_sub_category', root_data.get('sub_sub_category')),
                'current_language': lang_code,
                'language_name': translation_data.get('language_name', SUPPORTED_LANGUAGES.get(lang_code, lang_code))
            })
        else:
            # 번역이 없으면 한국어 사용
            root_data.update({
                'display_title': root_data.get('group_name'),
                'display_main_category': root_data.get('main_category'),
                'display_sub_category': root_data.get('sub_category'),
                'display_sub_sub_category': root_data.get('sub_sub_category'),
                'current_language': 'ko',
                'language_name': '한국어'
            })
        
        return root_data
        
    except Exception as e:
        logger.error(f"비디오 조회 실패 ({group_id}, {lang_code}): {e}")
        return None

# ===========================
# 수료증 관련 함수
# ===========================
def create_certificate(user_uid: str, cert_id: str, lecture_title: str, pdf_url: str) -> None:
    """수료증 생성"""
    cert_ref = db.collection('users').document(user_uid) \
                 .collection('completedCertificates').document(cert_id)
    cert_ref.set({
        'lectureTitle': lecture_title,
        'issuedAt': firestore.SERVER_TIMESTAMP,
        'pdfUrl': pdf_url,
        'excelUpdated': False,
        'readyForExcel': True
    }, merge=True)

def add_to_master_excel(user_uid: str, cert_id: str) -> None:
    """마스터 엑셀에 수료증 정보 추가"""
    try:
        # 수료증 정보 조회
        cert_ref = db.collection('users').document(user_uid) \
                     .collection('completedCertificates').document(cert_id)
        cert_doc = cert_ref.get()
        
        if not cert_doc.exists:
            raise ValueError('수료증이 존재하지 않습니다.')
        
        cert_info = cert_doc.to_dict()
        pdf_url = cert_info.get('pdfUrl', '')
        if not pdf_url:
            raise ValueError('PDF URL이 없습니다.')
        
        lecture_title = cert_info.get('lectureTitle', cert_id)
        issued_at = cert_info.get('issuedAt')
        
        # 타임스탬프 변환
        if hasattr(issued_at, 'to_datetime'):
            issued_dt = issued_at.to_datetime()
        else:
            issued_dt = datetime.utcnow()
        
        # 마스터 엑셀 다운로드/생성
        master_blob_name = 'master_certificates.xlsx'
        master_blob = firebase_bucket.blob(master_blob_name)
        
        try:
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer = io.BytesIO(existing_bytes)
            df_master = pd.read_excel(excel_buffer, engine='openpyxl')
        except Exception:
            df_master = pd.DataFrame(columns=[
                '업데이트 날짜', '사용자 UID', '전화번호', '이메일',
                '사용자 이름', '강의 제목', '발급 일시', 'PDF URL'
            ])
        
        # 사용자 정보 조회
        user_ref = db.collection("users").document(user_uid)
        user_snapshot = user_ref.get()
        if user_snapshot.exists:
            user_data = user_snapshot.to_dict()
            user_name = user_data.get("name", "")
            user_phone = user_data.get("phone", "")
            user_email = user_data.get("email", "")
        else:
            user_name = user_phone = user_email = ""
        
        # 새 행 추가
        updated_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        issued_str = issued_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        new_row = pd.DataFrame([{
            '업데이트 날짜': updated_date,
            '사용자 UID': user_uid,
            '전화번호': user_phone,
            '이메일': user_email,
            '사용자 이름': user_name,
            '강의 제목': lecture_title,
            '발급 일시': issued_str,
            'PDF URL': pdf_url
        }])
        df_master = pd.concat([df_master, new_row], ignore_index=True)
        
        # 엑셀 저장
        out_buffer = io.BytesIO()
        with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
            df_master.to_excel(writer, index=False, sheet_name="Certificates")
        out_buffer.seek(0)
        
        # Firebase Storage에 업로드
        master_blob.upload_from_file(
            out_buffer,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # 플래그 업데이트
        cert_ref.update({
            "excelUpdated": True,
            "readyForExcel": False
        })
        
        logger.info(f"✅ 마스터 엑셀에 추가 완료: {user_uid}/{cert_id}")
        
    except Exception as e:
        logger.error(f"마스터 엑셀 추가 실패: {e}")
        raise

# ===========================
# API 라우트
# ===========================
@app.route('/api/admin/login', methods=['POST'])
def api_admin_login():
    """관리자 로그인 API"""
    data = request.get_json() or {}
    email = data.get('email', '').strip()
    password = data.get('password', '')
    
    if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
        token = create_jwt_for_admin()
        return jsonify({'token': token}), 200
    else:
        return jsonify({'error': '관리자 인증 실패'}), 401

@app.route('/api/admin/refresh-urls', methods=['POST'])
@admin_required
def manual_refresh_urls():
    """수동으로 URL 갱신 트리거"""
    try:
        thread = threading.Thread(target=refresh_expiring_urls)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'message': 'URL 갱신 작업이 백그라운드에서 시작되었습니다.',
            'status': 'started'
        }), 200
        
    except Exception as e:
        logger.error(f"수동 URL 갱신 실패: {e}")
        return jsonify({'error': '갱신 작업 시작에 실패했습니다.'}), 500

@app.route('/api/admin/scheduler-status', methods=['GET'])
@admin_required
def get_scheduler_status():
    """스케줄러 상태 확인"""
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
        logger.error(f"스케줄러 상태 조회 실패: {e}")
        return jsonify({'error': '스케줄러 상태를 가져올 수 없습니다.'}), 500

@app.route('/api/videos/<group_id>', methods=['GET'])
def get_video_detail(group_id: str):
    """비디오 상세 정보 API (플러터 앱용)"""
    lang_code = request.args.get('lang', 'ko')
    
    if lang_code not in SUPPORTED_LANGUAGES:
        lang_code = 'ko'
    
    video_data = get_video_with_translation(group_id, lang_code)
    if not video_data:
        return jsonify({'error': '비디오를 찾을 수 없습니다.'}), 404
    
    # URL 갱신 확인
    current_presigned = video_data.get('presigned_url', '')
    if not current_presigned or is_presigned_url_expired(current_presigned, 60):
        new_presigned_url = generate_presigned_url(video_data['video_key'], expires_in=604800)
        db.collection('uploads').document(group_id).update({
            'presigned_url': new_presigned_url,
            'updated_at': datetime.utcnow().isoformat()
        })
        video_data['presigned_url'] = new_presigned_url
    
    return jsonify({
        'group_id': video_data['group_id'],
        'title': video_data['display_title'],
        'main_category': video_data['display_main_category'],
        'sub_category': video_data['display_sub_category'],
        'sub_sub_category': video_data['display_sub_sub_category'],
        'time': video_data['time'],
        'level': video_data['level'],
        'tag': video_data.get('tag', ''),
        'video_url': video_data['presigned_url'],
        'thumbnail_url': video_data.get('thumbnail_url', ''),
        'qr_url': video_data.get('qr_presigned_url', ''),
        'language': lang_code,
        'language_name': SUPPORTED_LANGUAGES[lang_code]
    })

@app.route('/api/videos/batch', methods=['POST'])
def get_videos_batch():
    """여러 비디오 정보를 한번에 조회 (플러터 앱 최적화)"""
    data = request.get_json() or {}
    group_ids = data.get('ids', [])
    lang = data.get('lang', 'ko')
    
    if not group_ids:
        return jsonify({'error': 'ids 파라미터가 필요합니다.'}), 400
    
    videos = []
    for group_id in group_ids[:50]:  # 최대 50개 제한
        video_data = get_video_with_translation(group_id, lang)
        if video_data:
            videos.append({
                'group_id': video_data['group_id'],
                'title': video_data['display_title'],
                'thumbnail_url': video_data.get('thumbnail_url', ''),
                'video_url': video_data['presigned_url'],
                'time': video_data['time'],
                'level': video_data['level'],
                'main_category': video_data.get('display_main_category'),
                'sub_category': video_data.get('display_sub_category')
            })
    
    return jsonify(videos)

@app.route('/api/videos', methods=['GET'])
def get_videos_list():
    """비디오 목록 조회 (페이지네이션)"""
    page = int(request.args.get('page', 0))
    limit = min(int(request.args.get('limit', 20)), 100)
    lang = request.args.get('lang', 'ko')
    
    try:
        # Firestore 쿼리
        query = db.collection('uploads') \
                  .order_by('created_at', direction=firestore.Query.DESCENDING) \
                  .limit(limit) \
                  .offset(page * limit)
        
        docs = query.stream()
        videos = []
        
        for doc in docs:
            video_data = doc.to_dict()
            # 번역 정보 추가
            translation = get_translation(video_data['group_id'], lang)
            
            if translation:
                title = translation.get('title', video_data['group_name'])
                main_cat = translation.get('main_category', video_data.get('main_category'))
                sub_cat = translation.get('sub_category', video_data.get('sub_category'))
            else:
                title = video_data['group_name']
                main_cat = video_data.get('main_category')
                sub_cat = video_data.get('sub_category')
            
            videos.append({
                'group_id': video_data['group_id'],
                'title': title,
                'main_category': main_cat,
                'sub_category': sub_cat,
                'thumbnail_url': video_data.get('thumbnail_url', ''),
                'video_url': video_data['presigned_url'],
                'time': video_data['time'],
                'level': video_data['level'],
                'tag': video_data.get('tag', '')
            })
        
        return jsonify({
            'videos': videos,
            'page': page,
            'limit': limit,
            'has_more': len(videos) == limit
        })
        
    except Exception as e:
        logger.error(f"비디오 목록 조회 실패: {e}")
        return jsonify({'error': '비디오 목록을 조회할 수 없습니다.'}), 500

@app.route('/api/create_certificate', methods=['POST'])
def api_create_certificate():
    """수료증 생성 API"""
    data = request.get_json() or {}
    user_uid = data.get('user_uid')
    cert_id = data.get('cert_id')
    lecture_title = data.get('lectureTitle', '')
    pdf_url = data.get('pdfUrl', '')
    
    if not user_uid or not cert_id or not pdf_url:
        return jsonify({'error': 'user_uid, cert_id, lectureTitle, pdfUrl이 필요합니다.'}), 400
    
    create_certificate(user_uid, cert_id, lecture_title, pdf_url)
    
    return jsonify({'message': '수료증이 생성되었습니다.'}), 200

@app.route('/api/add_certificate_to_master', methods=['POST'])
def api_add_certificate_to_master():
    """마스터 엑셀에 수료증 추가"""
    data = request.get_json() or {}
    user_uid = data.get('user_uid')
    cert_id = data.get('cert_id')
    
    if not user_uid or not cert_id:
        return jsonify({'error': 'user_uid와 cert_id가 필요합니다.'}), 400
    
    try:
        add_to_master_excel(user_uid, cert_id)
        return jsonify({'message': '마스터 엑셀에 추가되었습니다.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ===========================
# 웹 라우트
# ===========================
@app.route('/')
def login_page():
    """관리자 로그인 페이지"""
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    """로그인 처리"""
    pw = request.form.get('password', '')
    email = request.form.get('email', '')
    
    if email == ADMIN_EMAIL and pw == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('upload_form'))
    return render_template('login.html', error="이메일 또는 비밀번호가 올바르지 않습니다.")

@app.route('/upload_form')
@session_required
def upload_form():
    """비디오 업로드 폼"""
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
        '수공구': ['드릴', '해머', '플라이어'],
        '전동공구': ['그라인더', '전동 드릴', '해머드릴'],
        '절삭공구': ['커터', '플라즈마 노즐', '드릴 비트'],
        '안전장비': ['헬멧', '방진 마스크', '낙하 방지벨트'],
        '운송장비': ['리프트 장비', '체인 블록', '호이스트'],
        '작업장비': ['스캐폴딩', '작업대', '리프트 테이블']
    }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map, leafs=leaf_map)

@app.route('/upload', methods=['POST'])
@session_required
def upload_video():
    """비디오 업로드 처리"""
    file = request.files.get('file')
    thumbnail = request.files.get('thumbnail')
    group_name = request.form.get('group_name', 'default')
    main_cat = request.form.get('main_category', '')
    sub_cat = request.form.get('sub_category', '')
    leaf_cat = request.form.get('sub_sub_category', '')
    level = request.form.get('level', '')
    tag = request.form.get('tag', '')
    
    if not file:
        return "파일이 필요합니다.", 400
    
    try:
        result = process_video_upload(
            file, group_name, main_cat, sub_cat, leaf_cat, level, tag, thumbnail
        )
        
        return render_template(
            'success.html',
            group_id=result['group_id'],
            translations=result['translations'],
            time=result['time'],
            level=result['level'],
            tag=result['tag'],
            presigned_url=result['presigned_url'],
            thumbnail_url=result.get('thumbnail_url', ''),
            qr_url=result['qr_url']
        )
        
    except Exception as e:
        logger.error(f"업로드 실패: {e}")
        return f"업로드 실패: {str(e)}", 500

@app.route('/watch/<group_id>')
def watch(group_id: str):
    """비디오 시청 페이지 (웹/플러터 자동 감지)"""
    requested_lang = request.args.get('lang', 'ko')
    
    if requested_lang not in SUPPORTED_LANGUAGES:
        requested_lang = 'ko'
    
    # User Agent로 플러터 앱 감지
    user_agent = request.headers.get('User-Agent', '').lower()
    is_flutter_app = 'flutter' in user_agent or 'dart' in user_agent
    
    video_data = get_video_with_translation(group_id, requested_lang)
    if not video_data:
        abort(404)
    
    # URL 갱신
    current_presigned = video_data.get('presigned_url', '')
    if not current_presigned or is_presigned_url_expired(current_presigned, 60):
        new_presigned_url = generate_presigned_url(video_data['video_key'], expires_in=604800)
        db.collection('uploads').document(group_id).update({
            'presigned_url': new_presigned_url,
            'updated_at': datetime.utcnow().isoformat()
        })
        video_data['presigned_url'] = new_presigned_url
    
    if is_flutter_app:
        # Flutter 앱용 JSON 응답
        return jsonify({
            'groupId': group_id,
            'title': video_data['display_title'],
            'main_category': video_data['display_main_category'],
            'sub_category': video_data['display_sub_category'],
            'video_url': video_data['presigned_url'],
            'thumbnail_url': video_data.get('thumbnail_url', ''),
            'qr_url': video_data.get('qr_presigned_url', ''),
            'language': requested_lang,
            'time': video_data.get('time', '0:00'),
            'level': video_data.get('level', ''),
            'tag': video_data.get('tag', '')
        })
    else:
        # 웹 브라우저용 HTML 응답
        return render_template(
            'watch.html',
            video_url=video_data['presigned_url'],
            video_data=video_data,
            available_languages=SUPPORTED_LANGUAGES,
            current_language=requested_lang
        )

@app.route('/health', methods=['GET'])
def health_check():
    """서비스 상태 확인 엔드포인트"""
    try:
        # Firestore 연결 확인
        db.collection('uploads').limit(1).get()
        firestore_status = 'healthy'
    except Exception:
        firestore_status = 'unhealthy'
    
    try:
        # S3 연결 확인
        s3.head_bucket(Bucket=BUCKET_NAME)
        s3_status = 'healthy'
    except Exception:
        s3_status = 'unhealthy'
    
    overall_status = 'healthy' if (firestore_status == 'healthy' and s3_status == 'healthy') else 'unhealthy'
    
    return jsonify({
        'status': overall_status,
        'timestamp': datetime.utcnow().isoformat(),
        'services': {
            'firestore': firestore_status,
            's3': s3_status,
            'scheduler': scheduler.running
        },
        'version': '3.0.0-unified'
    }), 200 if overall_status == 'healthy' else 503

# ===========================
# 앱 초기화 및 실행
# ===========================
def initialize_app():
    """앱 초기화 함수"""
    try:
        # 디렉토리 생성
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        os.makedirs('fonts', exist_ok=True)
        
        # 한국어 폰트 다운로드
        download_korean_font()
        
        # Railway 환경 확인
        if os.environ.get('RAILWAY_ENVIRONMENT'):
            app.logger.setLevel(logging.INFO)
            app.logger.info("🚂 Railway 환경에서 실행 중")
        
        app.logger.info("✅ 앱 초기화 완료")
        return True
        
    except Exception as e:
        app.logger.error(f"❌ 앱 초기화 실패: {e}")
        return False

if __name__ == "__main__":
    # 앱 초기화
    initialize_app()
    
    # 스케줄러 시작
    start_scheduler()
    
    # 서버 시작
    port = int(os.environ.get("PORT", 8080))
    
    if os.environ.get('RAILWAY_ENVIRONMENT'):
        # Railway 프로덕션 환경
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        # 로컬 개발 환경
        app.run(host="0.0.0.0", port=port, debug=True)