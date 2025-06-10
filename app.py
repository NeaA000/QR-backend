# backend/app.py

import os
import uuid
import re
import io
import tempfile
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, date

from flask import (
    Flask, request, render_template,
    redirect, url_for, session, abort, jsonify
)
import boto3
from boto3.s3.transfer import TransferConfig
import qrcode
from PIL import Image, ImageDraw, ImageFont
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

# ── 번역 관련 import 추가 ──
from googletrans import Translator
import time

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

# ==== 번역 관련 설정 ====
# 전역 번역기 인스턴스
translator = Translator()

# 지원 언어 코드 매핑
SUPPORTED_LANGUAGES = {
    'ko': '한국어',
    'en': 'English',
    'zh': '中文',
    'vi': 'Tiếng Việt',
    'th': 'ไทย',
    'uz': 'O\'zbek',
    'ja': '日本語'
}

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

# ==== 번역 유틸리티 함수들 ====

def translate_text(text, target_language):
    """
    Google Translate API를 사용해서 텍스트 번역
    
    Args:
        text: 번역할 텍스트 (한국어)
        target_language: 대상 언어 코드
    
    Returns:
        번역된 텍스트 또는 원본 텍스트 (실패 시)
    """
    try:
        if target_language == 'ko' or not text.strip():
            return text  # 한국어는 원본 그대로, 빈 텍스트도 그대로
        
        # 번역 요청 (한국어 → 대상 언어)
        result = translator.translate(text, src='ko', dest=target_language)
        translated_text = result.text
        
        app.logger.info(f"번역 완료: '{text}' → '{translated_text}' ({target_language})")
        return translated_text
        
    except Exception as e:
        app.logger.warning(f"번역 실패 ({target_language}): {e}, 원본 텍스트 사용")
        return text

def create_multilingual_metadata(korean_text):
    """
    한국어 텍스트를 모든 지원 언어로 번역
    
    Args:
        korean_text: 번역할 한국어 텍스트
    
    Returns:
        Dict: 언어별 번역 결과
    """
    translations = {}
    
    if not korean_text.strip():
        # 빈 텍스트면 모든 언어에 빈 문자열 반환
        return {lang: '' for lang in SUPPORTED_LANGUAGES.keys()}
    
    for lang_code in SUPPORTED_LANGUAGES.keys():
        try:
            translated = translate_text(korean_text, lang_code)
            translations[lang_code] = translated
            
            # API 제한 방지를 위한 짧은 대기
            if lang_code != 'ko':
                time.sleep(0.2)
                
        except Exception as e:
            app.logger.error(f"언어 {lang_code} 번역 중 오류: {e}")
            translations[lang_code] = korean_text
    
    return translations

# ==== 기존 유틸리티 함수들 ====

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

def download_korean_font():
    """
    Railway 환경에서 한국어 폰트 다운로드 및 설정
    """
    font_dir = Path("fonts")
    font_dir.mkdir(exist_ok=True)
    
    font_path = font_dir / "NotoSansKR-Regular.ttf"
    
    if font_path.exists():
        return str(font_path)
    
    try:
        font_url = "https://fonts.gstatic.com/s/notosanskr/v27/PbykFmXiEBPT4ITbgNA5Cgm20xz64px_1hVWr0wuPNGmlQNMEfD4.ttf"
        app.logger.info("📥 한국어 폰트 다운로드 중...")
        urllib.request.urlretrieve(font_url, font_path)
        app.logger.info(f"✅ 폰트 다운로드 완료: {font_path}")
        return str(font_path)
    except Exception as e:
        app.logger.error(f"❌ 폰트 다운로드 실패: {e}")
        return None

def get_korean_font(size=24):
    """
    Railway 환경에서 한국어 폰트 로드
    """
    try:
        # 1. 다운로드된 한국어 폰트 시도
        korean_font_path = download_korean_font()
        if korean_font_path and os.path.exists(korean_font_path):
            return ImageFont.truetype(korean_font_path, size)
        
        # 2. Railway/Linux 환경 폰트 경로들
        linux_korean_fonts = [
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf'
        ]
        
        for font_path in linux_korean_fonts:
            if os.path.exists(font_path):
                try:
                    return ImageFont.truetype(font_path, size)
                except Exception:
                    continue
        
        app.logger.warning("⚠️ 한국어 폰트를 찾을 수 없어 기본 폰트 사용")
        return ImageFont.load_default()
        
    except Exception as e:
        app.logger.error(f"폰트 로드 실패: {e}")
        return ImageFont.load_default()

def get_text_dimensions(text, font, draw):
    """텍스트 크기 계산 (Pillow 버전 호환성)"""
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    except AttributeError:
        return draw.textsize(text, font=font)

def split_korean_text(text, font, max_width, draw):
    """한국어 텍스트를 폭에 맞게 분할"""
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = current_line + (" " if current_line else "") + word
        test_width, _ = get_text_dimensions(test_line, font, draw)
        
        if test_width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
                current_line = word
            else:
                # 단어가 너무 긴 경우 강제 분할
                while word:
                    test_width, _ = get_text_dimensions(word, font, draw)
                    if test_width <= max_width:
                        lines.append(word)
                        break
                    # 글자 단위로 분할
                    for i in range(len(word), 0, -1):
                        substr = word[:i]
                        test_width, _ = get_text_dimensions(substr, font, draw)
                        if test_width <= max_width:
                            lines.append(substr)
                            word = word[i:]
                            break
                current_line = ""
    
    if current_line:
        lines.append(current_line)
    
    return lines

def create_qr_with_logo(link_url, output_path, logo_path='static/logo.png', size_ratio=0.25, lecture_title=""):
    """
    개선된 QR 코드 생성 - Railway 환경 최적화 (한국어 폰트 지원)
    
    Args:
        link_url: QR 코드에 담을 URL
        output_path: 저장할 경로
        logo_path: 로고 이미지 경로
        size_ratio: 로고 크기 비율
        lecture_title: 하단에 표시할 강의명
    """
    from PIL import ImageDraw, ImageFont
    
    # QR 코드 생성 (더 큰 크기와 높은 오류 복구)
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=12,  # 박스 크기 증가
        border=4,
    )
    qr.add_data(link_url)
    qr.make(fit=True)
    
    # QR 이미지 생성 및 크기 조정
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    qr_size = 500  # 고정 크기로 더 크게
    qr_img = qr_img.resize((qr_size, qr_size), Image.LANCZOS)
    qr_w, qr_h = qr_img.size

    # 로고 삽입
    if os.path.exists(logo_path):
        try:
            logo = Image.open(logo_path)
            logo_size = int(qr_w * size_ratio)
            logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
            pos = ((qr_w - logo_size) // 2, (qr_h - logo_size) // 2)
            qr_img.paste(logo, pos, mask=(logo if logo.mode == 'RGBA' else None))
        except Exception as e:
            app.logger.warning(f"로고 삽입 실패: {e}")

    # 강의명 텍스트 추가 (개선된 한국어 지원)
    if lecture_title.strip():
        # 텍스트 영역을 충분히 크게 설정
        text_height = int(qr_h * 0.3)  # QR 코드 높이의 30%
        margin = int(qr_h * 0.04)      # 여백 4%
        
        # 새 이미지 생성
        total_height = qr_h + text_height + margin
        final_img = Image.new('RGB', (qr_w, total_height), 'white')
        final_img.paste(qr_img, (0, 0))
        
        # 텍스트 그리기 준비
        draw = ImageDraw.Draw(final_img)
        
        # 한국어 폰트 로드 (충분히 큰 크기)
        base_font_size = max(28, int(text_height * 0.18))  # 최소 28px
        font = get_korean_font(base_font_size)
        
        # 텍스트 영역 계산
        max_width = qr_w - 60  # 좌우 여백 30px씩
        
        # 텍스트를 여러 줄로 분할
        lines = split_korean_text(lecture_title, font, max_width, draw)
        
        # 3줄 이상이면 폰트 크기 조정
        if len(lines) > 3:
            font_size = max(22, int(base_font_size * 0.75))
            font = get_korean_font(font_size)
            lines = split_korean_text(lecture_title, font, max_width, draw)
            
            # 여전히 3줄 이상이면 자르기
            if len(lines) > 3:
                lines = lines[:2]
                last_line = lines[1]
                while True:
                    test_text = last_line + "..."
                    test_width, _ = get_text_dimensions(test_text, font, draw)
                    if test_width <= max_width or len(last_line) <= 3:
                        lines[1] = test_text
                        break
                    last_line = last_line[:-1]
        
        # 최대 3줄로 제한
        lines = lines[:3]
        
        # 텍스트 배치 계산
        _, line_height = get_text_dimensions("한글Ag", font, draw)
        total_text_height = len(lines) * line_height + (len(lines) - 1) * 8  # 줄간격 8px
        text_y_start = qr_h + margin + (text_height - total_text_height) // 2
        
        # 텍스트 그리기
        for i, line in enumerate(lines):
            if not line.strip():
                continue
                
            text_width, _ = get_text_dimensions(line, font, draw)
            text_x = (qr_w - text_width) // 2  # 중앙 정렬
            text_y = text_y_start + (i * (line_height + 8))
            
            # 가독성을 위한 흰색 외곽선 (선택사항)
            outline_offset = 2
            for dx in [-outline_offset, 0, outline_offset]:
                for dy in [-outline_offset, 0, outline_offset]:
                    if dx != 0 or dy != 0:
                        draw.text((text_x + dx, text_y + dy), line, font=font, fill='white')
            
            # 메인 텍스트 (검은색)
            draw.text((text_x, text_y), line, font=font, fill='black')
        
        # 고품질로 저장
        final_img.save(output_path, quality=95, optimize=True, dpi=(300, 300))
        app.logger.info(f"✅ 개선된 QR 코드 생성 완료: {lecture_title} (크기: {qr_w}x{total_height})")
        
    else:
        # 강의명이 없으면 QR 코드만 저장
        qr_img.save(output_path, quality=95, optimize=True)
        app.logger.info("✅ QR 코드 생성 완료 (강의명 없음)")

def initialize_korean_fonts():
    """앱 시작 시 한국어 폰트 환경 초기화"""
    try:
        font_dir = Path("fonts")
        font_dir.mkdir(exist_ok=True)
        
        # 한국어 폰트 미리 다운로드
        download_korean_font()
        
        app.logger.info("✅ 한국어 폰트 환경 초기화 완료")
        return True
    except Exception as e:
        app.logger.error(f"❌ 한국어 폰트 초기화 실패: {e}")
        return False

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
# 개선된 다국어 처리 함수들
# ===================================================================

def get_video_with_translation(group_id, lang_code='ko'):
    """
    특정 언어로 비디오 정보 조회
    
    Args:
        group_id: 비디오 그룹 ID
        lang_code: 언어 코드 (기본값: 'ko')
    
    Returns:
        dict: 루트 데이터 + 해당 언어 번역 데이터
    """
    try:
        # 1) 루트 문서 조회
        root_doc = db.collection('uploads').document(group_id).get()
        if not root_doc.exists:
            return None
        
        root_data = root_doc.to_dict()
        
        # 2) 번역 문서 조회
        translation_doc = db.collection('uploads').document(group_id) \
                           .collection('translations').document(lang_code).get()
        
        if translation_doc.exists:
            translation_data = translation_doc.to_dict()
            # 번역 데이터를 루트 데이터에 오버라이드
            root_data.update({
                'display_title': translation_data.get('title', root_data.get('group_name')),
                'display_main_category': translation_data.get('main_category', root_data.get('main_category')),
                'display_sub_category': translation_data.get('sub_category', root_data.get('sub_category')),
                'display_sub_sub_category': translation_data.get('sub_sub_category', root_data.get('sub_sub_category')),
                'current_language': lang_code,
                'language_name': translation_data.get('language_name', SUPPORTED_LANGUAGES.get(lang_code, lang_code))
            })
        else:
            # 번역이 없으면 한국어(원본) 사용
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
        app.logger.error(f"비디오 조회 실패 ({group_id}, {lang_code}): {e}")
        return None

def add_language_to_existing_videos(new_lang_code, new_lang_name):
    """
    기존 비디오들에 새로운 언어 번역 추가
    
    Args:
        new_lang_code: 새 언어 코드 (예: 'fr')
        new_lang_name: 새 언어 이름 (예: 'Français')
    """
    try:
        # 모든 업로드 문서 조회
        uploads = db.collection('uploads').stream()
        
        for doc in uploads:
            root_data = doc.to_dict()
            group_id = doc.id
            
            # 한국어 원본 텍스트들
            korean_title = root_data.get('group_name', '')
            korean_main_cat = root_data.get('main_category', '')
            korean_sub_cat = root_data.get('sub_category', '')
            korean_leaf_cat = root_data.get('sub_sub_category', '')
            
            # 새 언어로 번역
            translated_title = translate_text(korean_title, new_lang_code)
            translated_main = translate_text(korean_main_cat, new_lang_code)
            translated_sub = translate_text(korean_sub_cat, new_lang_code)
            translated_leaf = translate_text(korean_leaf_cat, new_lang_code)
            
            # 새 번역 문서 생성
            translation_data = {
                'title': translated_title,
                'main_category': translated_main,
                'sub_category': translated_sub,
                'sub_sub_category': translated_leaf,
                'language_code': new_lang_code,
                'language_name': new_lang_name,
                'is_original': False,
                'translated_at': datetime.utcnow().isoformat()
            }
            
            # 번역 서브컬렉션에 추가
            db.collection('uploads').document(group_id) \
              .collection('translations').document(new_lang_code) \
              .set(translation_data)
            
            app.logger.info(f"언어 추가 완료: {group_id} -> {new_lang_code}")
            
            # API 제한 방지
            time.sleep(0.3)
        
        app.logger.info(f"✅ 모든 비디오에 {new_lang_name}({new_lang_code}) 언어 추가 완료")
        
    except Exception as e:
        app.logger.error(f"언어 추가 실패: {e}")

# ===================================================================
# 백그라운드 자동 갱신 시스템
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
                    
                    doc.reference.update(update_data)
                    
                    updated_count += 1
                    app.logger.info(f"✅ 문서 {doc.id} URL 갱신 완료")
                    
                except Exception as update_error:
                    app.logger.error(f"❌ 문서 {doc.id} URL 갱신 실패: {update_error}")
        
        app.logger.info(f"🎉 백그라운드 URL 갱신 완료: {updated_count}/{total_count} 개 갱신됨")
        
    except Exception as e:
        app.logger.error(f"❌ 백그라운드 URL 갱신 작업 중 오류: {e}")

def refresh_qr_presigned_urls():
    """
    QR 이미지의 presigned URL도 갱신 (단일 QR 이미지)
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
# 개선된 업로드 핸들러: 서브컬렉션 번역 구조
# ===================================================================
@app.route('/upload', methods=['POST'])
def upload_video():
    """
    개선된 업로드 처리: 루트 문서와 번역 서브컬렉션 분리
    1) 클라이언트에서 파일과 기타 메타데이터 수신
    2) 한국어 강의명을 7개 언어로 자동 번역
    3) 파일을 임시로 저장 → S3 업로드
    4) moviepy로 동영상 길이(초 단위) 계산 → "분:초" 문자열로 변환
    5) 루트 문서에 핵심 메타데이터 저장
    6) 번역 서브컬렉션에 언어별 번역 저장
    """
    # 세션 인증(기존 로직)
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file          = request.files.get('file')
    group_name    = request.form.get('group_name', 'default')  # 한국어 강의명
    main_cat      = request.form.get('main_category', '')
    sub_cat       = request.form.get('sub_category', '')
    leaf_cat      = request.form.get('sub_sub_category', '')
    lecture_level = request.form.get('level', '')
    lecture_tag   = request.form.get('tag', '')

    if not file:
        return "파일이 필요합니다.", 400

    # 🌍 1) 한국어 강의명을 7개 언어로 번역
    app.logger.info(f"다국어 번역 시작: '{group_name}'")
    translated_titles = create_multilingual_metadata(group_name)
    
    # 카테고리들도 번역 (선택사항)
    translated_main_cat = create_multilingual_metadata(main_cat) if main_cat else {}
    translated_sub_cat = create_multilingual_metadata(sub_cat) if sub_cat else {}
    translated_leaf_cat = create_multilingual_metadata(leaf_cat) if leaf_cat else {}

    # 2) 그룹 ID 생성 및 S3 키 구성
    group_id = uuid.uuid4().hex
    date_str = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder = f"videos/{group_id}_{safe_name}_{date_str}"
    ext = Path(file.filename).suffix or '.mp4'
    video_key = f"{folder}/video{ext}"

    # 3) 임시 저장 및 S3 업로드
    tmp_path = Path(tempfile.gettempdir()) / f"{group_id}{ext}"
    file.save(tmp_path)

    # 4) moviepy를 사용해 동영상 길이 계산
    try:
        with VideoFileClip(str(tmp_path)) as clip:
            duration_sec = int(clip.duration)
    except Exception as e:
        duration_sec = 0
        app.logger.warning(f"moviepy로 동영상 길이 가져오기 실패: {e}")

    # "분:초" 형식으로 변환
    minutes = duration_sec // 60
    seconds = duration_sec % 60
    lecture_time = f"{minutes}:{seconds:02d}"
    app.logger.info(f"계산된 동영상 길이: {lecture_time} (총 {duration_sec}초)")

    # S3 업로드
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    # 5) Presigned URL 생성
    presigned_url = generate_presigned_url(video_key, expires_in=604800)

    # 6) 단일 QR 코드 생성 (한국어 기본)
    qr_link = f"{APP_BASE_URL}{group_id}"  # 언어 파라미터 없이
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    
    # 한국어 강의명으로 QR 코드 생성
    display_title = group_name
    if main_cat or sub_cat or leaf_cat:
        categories = [cat for cat in [main_cat, sub_cat, leaf_cat] if cat]
        if categories:
            display_title = f"{group_name}\n({' > '.join(categories)})"
    
    create_qr_with_logo(qr_link, local_qr, lecture_title=display_title)
    
    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)
    
    # QR 이미지 URL 생성
    qr_presigned_url = generate_presigned_url(qr_key, expires_in=604800)
    
    # 로컬 파일 삭제
    try:
        os.remove(local_qr)
    except OSError:
        pass

    # 📝 7) 루트 문서 저장 (핵심 메타데이터만)
    root_doc_data = {
        'group_id': group_id,
        'group_name': group_name,           # 기본 언어(한국어)
        'main_category': main_cat,
        'sub_category': sub_cat,
        'sub_sub_category': leaf_cat,
        'time': lecture_time,
        'level': lecture_level,
        'tag': lecture_tag,
        'video_key': video_key,
        'presigned_url': presigned_url,
        'qr_link': qr_link,
        'qr_key': qr_key,
        'qr_presigned_url': qr_presigned_url,
        'upload_date': date_str,
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat()
    }

    # 루트 문서 저장
    root_doc_ref = db.collection('uploads').document(group_id)
    root_doc_ref.set(root_doc_data)

    # 📝 8) 번역 서브컬렉션 저장 (언어별로 분리)
    translations_ref = root_doc_ref.collection('translations')
    
    for lang_code in SUPPORTED_LANGUAGES.keys():
        if lang_code == 'ko':
            # 한국어는 원본 그대로
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
            # 번역된 언어들
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
        
        # 각 언어별 문서 저장
        translations_ref.document(lang_code).set(translation_data)
        app.logger.info(f"번역 저장 완료: {lang_code} - {translation_data.get('title')}")

    app.logger.info(f"✅ 개선된 구조로 업로드 완료: {group_id}")
    app.logger.info(f"번역된 언어: {list(translated_titles.keys())}")

    return render_template(
        'success.html',
        group_id=group_id,
        translations=translated_titles,
        time=lecture_time,
        level=lecture_level,
        tag=lecture_tag,
        presigned_url=presigned_url,
        qr_url=qr_presigned_url
    )

# ===================================================================
# 수료증 정보 생성 시 Firestore에 readyForExcel & excelUpdated 플래그 추가
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

# ===================================================================
# 개선된 라우팅 설정
# ===================================================================

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
    개선된 동영상 시청 페이지: 언어별 번역 서브컬렉션 활용
    Flutter 앱에서 언어를 동적으로 변경 가능
    """
    # URL에서 언어 파라미터 확인
    requested_lang = request.args.get('lang', 'ko')
    
    # 지원하지 않는 언어면 한국어로 폴백
    if requested_lang not in SUPPORTED_LANGUAGES:
        requested_lang = 'ko'
    
    # 비디오 데이터 조회 (번역 포함)
    video_data = get_video_with_translation(group_id, requested_lang)
    if not video_data:
        abort(404)

    # Presigned URL 갱신 로직 (기존과 동일)
    current_presigned = video_data.get('presigned_url', '')
    if not current_presigned or is_presigned_url_expired(current_presigned, 60):
        new_presigned_url = generate_presigned_url(video_data['video_key'], expires_in=604800)
        
        # 루트 문서만 업데이트
        db.collection('uploads').document(group_id).update({
            'presigned_url': new_presigned_url,
            'updated_at': datetime.utcnow().isoformat()
        })
        video_data['presigned_url'] = new_presigned_url

    # QR URL도 갱신 확인
    current_qr_url = video_data.get('qr_presigned_url', '')
    qr_key = video_data.get('qr_key', '')
    if qr_key and (not current_qr_url or is_presigned_url_expired(current_qr_url, 60)):
        new_qr_url = generate_presigned_url(qr_key, expires_in=604800)
        db.collection('uploads').document(group_id).update({
            'qr_presigned_url': new_qr_url,
            'qr_updated_at': datetime.utcnow().isoformat()
        })
        video_data['qr_presigned_url'] = new_qr_url
    
    # 템플릿에 번역된 데이터 전달
    return render_template(
        'watch.html',
        video_url=video_data['presigned_url'],
        video_data=video_data,
        available_languages=SUPPORTED_LANGUAGES,
        current_language=requested_lang
    )

# ===================================================================
# 다국어 API 엔드포인트들
# ===================================================================

@app.route('/api/videos', methods=['GET'])
def get_videos_list():
    """
    비디오 목록 조회 API (다국어 지원)
    Query params:
    - lang: 언어 코드 (기본값: ko)
    - category: 카테고리 필터
    - level: 레벨 필터
    """
    lang_code = request.args.get('lang', 'ko')
    category_filter = request.args.get('category')
    level_filter = request.args.get('level')
    
    if lang_code not in SUPPORTED_LANGUAGES:
        lang_code = 'ko'
    
    try:
        # 루트 컬렉션에서 기본 필터링
        query = db.collection('uploads')
        
        if category_filter:
            query = query.where('main_category', '==', category_filter)
        if level_filter:
            query = query.where('level', '==', level_filter)
        
        docs = query.stream()
        
        videos = []
        for doc in docs:
            video_data = get_video_with_translation(doc.id, lang_code)
            if video_data:
                # 필요한 필드만 선택해서 응답 크기 최소화
                videos.append({
                    'group_id': video_data['group_id'],
                    'title': video_data['display_title'],
                    'main_category': video_data['display_main_category'],
                    'sub_category': video_data['display_sub_category'],
                    'sub_sub_category': video_data['display_sub_sub_category'],
                    'time': video_data['time'],
                    'level': video_data['level'],
                    'tag': video_data.get('tag', ''),
                    'upload_date': video_data['upload_date'],
                    'language': lang_code,
                    'qr_url': video_data.get('qr_presigned_url', '')
                })
        
        return jsonify({
            'videos': videos,
            'language': lang_code,
            'language_name': SUPPORTED_LANGUAGES[lang_code],
            'total': len(videos)
        })
        
    except Exception as e:
        app.logger.error(f"비디오 목록 조회 실패: {e}")
        return jsonify({'error': '비디오 목록을 가져올 수 없습니다.'}), 500

@app.route('/api/videos/<group_id>', methods=['GET'])
def get_video_detail(group_id):
    """
    특정 비디오 상세 정보 조회 API (다국어 지원)
    Query params:
    - lang: 언어 코드 (기본값: ko)
    """
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
        'qr_url': video_data.get('qr_presigned_url', ''),
        'qr_link': video_data.get('qr_link', ''),
        'language': lang_code,
        'language_name': SUPPORTED_LANGUAGES[lang_code],
        'upload_date': video_data['upload_date']
    })

@app.route('/api/admin/add-language', methods=['POST'])
@admin_required
def api_add_language():
    """
    관리자용: 새로운 언어 추가 API
    Body: { "language_code": "fr", "language_name": "Français" }
    """
    data = request.get_json() or {}
    lang_code = data.get('language_code', '').strip().lower()
    lang_name = data.get('language_name', '').strip()
    
    if not lang_code or not lang_name:
        return jsonify({'error': 'language_code와 language_name이 필요합니다.'}), 400
    
    if lang_code in SUPPORTED_LANGUAGES:
        return jsonify({'error': f'언어 코드 {lang_code}는 이미 지원됩니다.'}), 400
    
    try:
        # 백그라운드에서 실행
        thread = threading.Thread(
            target=add_language_to_existing_videos, 
            args=(lang_code, lang_name)
        )
        thread.daemon = True
        thread.start()
        
        # 전역 언어 목록에 추가
        SUPPORTED_LANGUAGES[lang_code] = lang_name
        
        return jsonify({
            'message': f'{lang_name}({lang_code}) 언어 추가 작업이 백그라운드에서 시작되었습니다.',
            'language_code': lang_code,
            'language_name': lang_name
        }), 200
        
    except Exception as e:
        app.logger.error(f"언어 추가 실패: {e}")
        return jsonify({'error': '언어 추가 작업 시작에 실패했습니다.'}), 500

@app.route('/api/languages', methods=['GET'])
def get_supported_languages():
    """
    지원하는 언어 목록 조회 API
    """
    return jsonify({
        'languages': SUPPORTED_LANGUAGES,
        'total': len(SUPPORTED_LANGUAGES)
    })

# ===================================================================
# 기존 ZIP 생성 엔드포인트들
# ===================================================================

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
# 데이터 마이그레이션 유틸리티 (기존 구조 → 새 구조)
# ===================================================================

@app.route('/api/admin/migrate-to-subcollections', methods=['POST'])
@admin_required
def migrate_to_subcollections():
    """
    관리자용: 기존 translations 필드를 서브컬렉션으로 마이그레이션
    
    기존 구조:
    uploads/{group_id} {
        translations: {
            title: { ko: "...", en: "...", ... },
            main_category: { ko: "...", en: "...", ... }
        }
    }
    
    새 구조:
    uploads/{group_id}/translations/{lang_code} {
        title: "...",
        main_category: "...",
        language_code: "...",
        language_name: "..."
    }
    """
    try:
        # 모든 업로드 문서 조회
        uploads = db.collection('uploads').stream()
        migrated_count = 0
        error_count = 0
        
        for doc in uploads:
            try:
                data = doc.to_dict()
                group_id = doc.id
                
                # 기존 translations 필드 확인
                old_translations = data.get('translations', {})
                if not old_translations:
                    app.logger.info(f"문서 {group_id}: translations 필드 없음, 건너뜀")
                    continue
                
                # 번역 서브컬렉션 생성
                translations_ref = doc.reference.collection('translations')
                
                # 언어별로 문서 생성
                for lang_code in SUPPORTED_LANGUAGES.keys():
                    translation_data = {
                        'title': old_translations.get('title', {}).get(lang_code, data.get('group_name', '')),
                        'main_category': old_translations.get('main_category', {}).get(lang_code, data.get('main_category', '')),
                        'sub_category': old_translations.get('sub_category', {}).get(lang_code, data.get('sub_category', '')),
                        'sub_sub_category': old_translations.get('sub_sub_category', {}).get(lang_code, data.get('sub_sub_category', '')),
                        'language_code': lang_code,
                        'language_name': SUPPORTED_LANGUAGES[lang_code],
                        'is_original': (lang_code == 'ko'),
                        'migrated_at': datetime.utcnow().isoformat(),
                        'migration_source': 'legacy_translations_field'
                    }
                    
                    translations_ref.document(lang_code).set(translation_data)
                
                # 기존 translations 필드 제거
                doc.reference.update({
                    'translations': firestore.DELETE_FIELD,
                    'migrated_to_subcollections': True,
                    'migration_completed_at': datetime.utcnow().isoformat()
                })
                
                migrated_count += 1
                app.logger.info(f"✅ 마이그레이션 완료: {group_id}")
                
            except Exception as doc_error:
                error_count += 1
                app.logger.error(f"❌ 문서 {group_id} 마이그레이션 실패: {doc_error}")
        
        return jsonify({
            'message': '마이그레이션이 완료되었습니다.',
            'migrated_count': migrated_count,
            'error_count': error_count,
            'total_processed': migrated_count + error_count
        }), 200
        
    except Exception as e:
        app.logger.error(f"마이그레이션 실패: {e}")
        return jsonify({'error': '마이그레이션 중 오류가 발생했습니다.'}), 500

@app.route('/api/admin/cleanup-old-translations', methods=['POST'])
@admin_required
def cleanup_old_translations():
    """
    관리자용: 마이그레이션 후 정리 작업
    - migrated_to_subcollections 플래그가 있는 문서들의 남은 translations 필드 제거
    """
    try:
        uploads = db.collection('uploads') \
                   .where('migrated_to_subcollections', '==', True) \
                   .stream()
        
        cleaned_count = 0
        
        for doc in uploads:
            data = doc.to_dict()
            if 'translations' in data:
                doc.reference.update({
                    'translations': firestore.DELETE_FIELD,
                    'cleaned_up_at': datetime.utcnow().isoformat()
                })
                cleaned_count += 1
                app.logger.info(f"✅ 정리 완료: {doc.id}")
        
        return jsonify({
            'message': '정리 작업이 완료되었습니다.',
            'cleaned_count': cleaned_count
        }), 200
        
    except Exception as e:
        app.logger.error(f"정리 작업 실패: {e}")
        return jsonify({'error': '정리 작업 중 오류가 발생했습니다.'}), 500

# ===================================================================
# 통계 및 모니터링 API
# ===================================================================

@app.route('/api/admin/stats', methods=['GET'])
@admin_required
def get_admin_stats():
    """
    관리자용 통계 대시보드 API
    """
    try:
        # 전체 비디오 수
        total_videos = len(list(db.collection('uploads').stream()))
        
        # 언어별 번역 완성도
        language_stats = {}
        for lang_code, lang_name in SUPPORTED_LANGUAGES.items():
            translation_count = 0
            uploads = db.collection('uploads').stream()
            
            for doc in uploads:
                translation_doc = doc.reference.collection('translations').document(lang_code).get()
                if translation_doc.exists:
                    translation_count += 1
            
            language_stats[lang_code] = {
                'name': lang_name,
                'translated_count': translation_count,
                'completion_rate': (translation_count / total_videos * 100) if total_videos > 0 else 0
            }
        
        # 최근 업로드 (7일)
        week_ago = datetime.utcnow() - timedelta(days=7)
        recent_uploads = 0
        uploads = db.collection('uploads').where('created_at', '>=', week_ago.isoformat()).stream()
        recent_uploads = len(list(uploads))
        
        return jsonify({
            'total_videos': total_videos,
            'supported_languages': len(SUPPORTED_LANGUAGES),
            'language_stats': language_stats,
            'recent_uploads_7days': recent_uploads,
            'scheduler_running': scheduler.running if 'scheduler' in globals() else False
        }), 200
        
    except Exception as e:
        app.logger.error(f"통계 조회 실패: {e}")
        return jsonify({'error': '통계를 가져올 수 없습니다.'}), 500

# ===================================================================
# 헬스체크 엔드포인트
# ===================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """
    서비스 상태 확인 엔드포인트
    """
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
            'scheduler': scheduler.running if 'scheduler' in globals() else False
        },
        'supported_languages': list(SUPPORTED_LANGUAGES.keys()),
        'version': '2.0.0-improved'
    }), 200 if overall_status == 'healthy' else 503

# Railway 환경을 위한 초기화 함수
def initialize_railway_environment():
    """Railway 배포 환경 초기화"""
    try:
        # 폰트 환경 초기화
        initialize_korean_fonts()
        
        # 정적 파일 디렉토리 확인
        os.makedirs('static', exist_ok=True)
        os.makedirs('fonts', exist_ok=True)
        
        # 환경별 로그 레벨 설정
        if os.environ.get('RAILWAY_ENVIRONMENT'):
            import logging
            app.logger.setLevel(logging.INFO)
        
        app.logger.info("🚂 Railway 환경 초기화 완료")
        return True
        
    except Exception as e:
        app.logger.error(f"❌ Railway 환경 초기화 실패: {e}")
        return False

# ===================================================================
# 앱 시작 시 스케줄러 자동 실행
# ===================================================================

if __name__ == "__main__":
    # Railway 환경 초기화
    initialize_railway_environment()
    
    # 스케줄러 시작
    start_background_scheduler()
    
    port = int(os.environ.get("PORT", 8080))
    
    # Railway 환경에서는 gunicorn 사용 권장
    if os.environ.get('RAILWAY_ENVIRONMENT'):
        # Railway에서는 gunicorn이 자동으로 처리
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        # 로컬 개발 환경
        app.run(host="0.0.0.0", port=port, debug=True)

# ===================================================================
# 추가 유틸리티 함수들
# ===================================================================

def batch_update_translations(updates_list):
    """
    번역 일괄 업데이트 유틸리티
    
    Args:
        updates_list: [
            {
                'group_id': 'abc123',
                'lang_code': 'en', 
                'updates': {'title': 'New Title', 'main_category': 'New Category'}
            },
            ...
        ]
    """
    try:
        batch = db.batch()
        
        for update_item in updates_list:
            group_id = update_item['group_id']
            lang_code = update_item['lang_code']
            updates = update_item['updates']
            
            translation_ref = db.collection('uploads').document(group_id) \
                               .collection('translations').document(lang_code)
            
            updates['updated_at'] = datetime.utcnow().isoformat()
            batch.update(translation_ref, updates)
        
        batch.commit()
        app.logger.info(f"✅ 일괄 번역 업데이트 완료: {len(updates_list)}개")
        return True
        
    except Exception as e:
        app.logger.error(f"일괄 번역 업데이트 실패: {e}")
        return False

def verify_translation_integrity():
    """
    번역 데이터 무결성 검증
    - 모든 비디오에 필요한 언어 번역이 있는지 확인
    - 누락된 번역 자동 생성
    """
    try:
        uploads = db.collection('uploads').stream()
        missing_translations = []
        
        for doc in uploads:
            root_data = doc.to_dict()
            group_id = doc.id
            
            # 각 언어별 번역 문서 확인
            for lang_code in SUPPORTED_LANGUAGES.keys():
                translation_doc = doc.reference.collection('translations').document(lang_code).get()
                
                if not translation_doc.exists:
                    missing_translations.append({
                        'group_id': group_id,
                        'lang_code': lang_code,
                        'korean_title': root_data.get('group_name', ''),
                        'korean_main_cat': root_data.get('main_category', ''),
                        'korean_sub_cat': root_data.get('sub_category', ''),
                        'korean_leaf_cat': root_data.get('sub_sub_category', '')
                    })
        
        app.logger.info(f"번역 무결성 검증 완료: {len(missing_translations)}개 누락 발견")
        return missing_translations
        
    except Exception as e:
        app.logger.error(f"번역 무결성 검증 실패: {e}")
        return None

# ===================================================================
# 개발/테스트용 엔드포인트 (운영에서는 제거 권장)
# ===================================================================

@app.route('/api/dev/test-translation', methods=['POST'])
def test_translation():
    """
    개발용: 번역 테스트 엔드포인트
    Body: { "text": "테스트할 텍스트", "target_lang": "en" }
    """
    if not app.debug:  # 디버그 모드에서만 사용 가능
        return jsonify({'error': '개발 모드에서만 사용 가능합니다.'}), 403
    
    data = request.get_json() or {}
    text = data.get('text', '')
    target_lang = data.get('target_lang', 'en')
    
    if not text:
        return jsonify({'error': 'text가 필요합니다.'}), 400
    
    try:
        translated = translate_text(text, target_lang)
        return jsonify({
            'original': text,
            'translated': translated,
            'target_language': target_lang,
            'target_language_name': SUPPORTED_LANGUAGES.get(target_lang, target_lang)
        })
    except Exception as e:
        return jsonify({'error': f'번역 실패: {e}'}), 500