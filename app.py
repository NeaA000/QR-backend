# backend/app.py - Flutter 완전 호환 버전 (언어별 영상 지원) - 관리자 인증 문제 해결

import os
import uuid
import re
import io
import tempfile
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, date
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
import json

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

# ── 변경된 부분: video 파일 길이를 가져오기 위한 import ──
from moviepy.video.io.VideoFileClip import VideoFileClip

# ── 번역 관련 import 추가 (보안 강화) ──
from googletrans import Translator
import time

# ==== 환경변수 설정 (보안 강화) ====
ADMIN_EMAIL       = os.environ.get('ADMIN_EMAIL', '')
ADMIN_PASSWORD    = os.environ.get('ADMIN_PASSWORD', 'changeme')
JWT_SECRET        = os.environ.get('JWT_SECRET', 'supersecretjwt')
JWT_ALGORITHM     = 'HS256'
JWT_EXPIRES_HOURS = 4

# AWS 설정 검증
required_aws_vars = ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'REGION_NAME', 'BUCKET_NAME']
for var in required_aws_vars:
    if not os.environ.get(var):
        raise ValueError(f"필수 환경변수 {var}가 설정되지 않았습니다.")

AWS_ACCESS_KEY    = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY    = os.environ['AWS_SECRET_KEY']
REGION_NAME       = os.environ['REGION_NAME']
BUCKET_NAME       = os.environ['BUCKET_NAME']
APP_BASE_URL      = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
SECRET_KEY        = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

# ==== 번역 관련 설정 - 수정됨 (보안 및 성능 최적화) ====
# 전역 번역기 인스턴스 (재사용으로 성능 향상)
translator = None
translation_lock = threading.Lock()

# 지원 언어 코드 매핑 - Flutter와 동일하게 맞춤
SUPPORTED_LANGUAGES = {
    'ko': '한국어',
    'en': 'English',
    'zh': '中文',        # Flutter와 일치
    'vi': 'Tiếng Việt',
    'th': 'ไทย',
    'ja': '日本語'
}

# 번역 캐시 (메모리 효율성)
translation_cache = {}
TRANSLATION_CACHE_SIZE = 1000

def get_translator():
    """Thread-safe translator 인스턴스 가져오기"""
    global translator
    if translator is None:
        with translation_lock:
            if translator is None:
                try:
                    translator = Translator()
                    # 연결 테스트
                    translator.translate("test", dest='en')
                except Exception as e:
                    app.logger.error(f"번역기 초기화 실패: {e}")
                    translator = None
    return translator

# ==== Firebase Admin + Firestore + Storage 초기화 (보안 강화) ====
def initialize_firebase():
    """Firebase 안전 초기화"""
    if firebase_admin._apps:
        return
    
    required_firebase_vars = [
        'type', 'project_id', 'private_key', 'client_email',
        'auth_uri', 'token_uri'
    ]
    
    for var in required_firebase_vars:
        if not os.environ.get(var):
            raise ValueError(f"필수 Firebase 환경변수 {var}가 설정되지 않았습니다.")
    
    try:
        firebase_creds = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ.get("private_key_id", ""),
            "private_key": os.environ["private_key"].replace('\\n', '\n'),
            "client_email": os.environ["client_email"],
            "client_id": os.environ.get("client_id", ""),
            "auth_uri": os.environ.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": os.environ.get("token_uri", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": os.environ.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": os.environ.get("client_x509_cert_url", "")
        }
        
        cred = credentials.Certificate(firebase_creds)
        firebase_admin.initialize_app(cred, {
            'storageBucket': f"{os.environ['project_id']}.appspot.com"
        })
        
    except Exception as e:
        raise RuntimeError(f"Firebase 초기화 실패: {e}")

# Firebase 초기화
initialize_firebase()
db = firestore.client()
bucket = storage.bucket()

# ==== Flask 앱 설정 (보안 강화) ====
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = 'static'
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB로 제한 (Railway 최적화)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000  # 1년 캐싱

# 보안 헤더 설정
@app.after_request
def after_request(response):
    """보안 헤더 추가"""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    # CORS 설정 (Flutter 앱 지원)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ==== Wasabi S3 클라이언트 설정 (성능 최적화) ====
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=f'https://s3.{REGION_NAME}.wasabisys.com'
)

config = TransferConfig(
    multipart_threshold=1024 * 1024 * 25,
    multipart_chunksize=1024 * 1024 * 25,  # 청크 크기 감소
    max_concurrency=3,  # 동시 연결 수 감소
    use_threads=True
)

# ==== 수정된 번역 유틸리티 함수들 (성능 및 안정성 강화) ====

def translate_text_safe(text, target_language, max_retries=2):
    """
    안전한 번역 함수 - 재시도 로직 및 캐싱 포함
    """
    if not text or not text.strip():
        return text
        
    if target_language == 'ko':
        return text
    
    # 캐시 확인
    cache_key = f"{text[:50]}_{target_language}"
    if cache_key in translation_cache:
        return translation_cache[cache_key]
    
    translator_instance = get_translator()
    if not translator_instance:
        app.logger.warning("번역기를 사용할 수 없어 원본 텍스트 반환")
        return text
    
    for attempt in range(max_retries):
        try:
            # 텍스트 길이 제한 (API 제한 고려)
            if len(text) > 500:
                text = text[:500] + "..."
            
            # 중국어 처리 수정
            dest_lang = 'zh' if target_language == 'zh' else target_language
            result = translator_instance.translate(text, src='ko', dest=dest_lang)
            translated_text = result.text
            
            # 캐시 저장 (크기 제한)
            if len(translation_cache) < TRANSLATION_CACHE_SIZE:
                translation_cache[cache_key] = translated_text
            
            app.logger.debug(f"번역 성공 ({target_language}): {len(text)}자")
            return translated_text
            
        except Exception as e:
            app.logger.warning(f"번역 시도 {attempt + 1} 실패 ({target_language}): {str(e)[:100]}")
            if attempt < max_retries - 1:
                time.sleep(1)  # 재시도 전 대기
            
    app.logger.warning(f"번역 최종 실패 ({target_language}), 원본 텍스트 사용")
    return text

def create_multilingual_metadata_async(korean_text):
    """
    비동기 다국어 번역 - 백그라운드에서 실행
    """
    def translate_worker():
        translations = {'ko': korean_text}  # 한국어는 원본
        
        if not korean_text.strip():
            return {lang: '' for lang in SUPPORTED_LANGUAGES.keys()}
        
        for lang_code in SUPPORTED_LANGUAGES.keys():
            if lang_code == 'ko':
                continue
                
            try:
                translated = translate_text_safe(korean_text, lang_code)
                translations[lang_code] = translated
                time.sleep(0.1)  # API 호출 간격
                
            except Exception as e:
                app.logger.error(f"언어 {lang_code} 번역 중 오류: {e}")
                translations[lang_code] = korean_text
        
        return translations
    
    # ThreadPoolExecutor로 백그라운드 실행
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(translate_worker)
        try:
            return future.result(timeout=10)  # 10초 타임아웃
        except Exception as e:
            app.logger.error(f"번역 작업 타임아웃: {e}")
            return {lang: korean_text for lang in SUPPORTED_LANGUAGES.keys()}

# ==== 기존 유틸리티 함수들 ====

def generate_presigned_url(key, expires_in=86400):
    """S3 객체에 대해 presigned URL 생성"""
    try:
        return s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': key},
            ExpiresIn=expires_in
        )
    except Exception as e:
        app.logger.error(f"Presigned URL 생성 실패: {e}")
        return ""

# ==== 수정된 한국어 폰트 함수들 (성능 및 안정성 대폭 개선) ====

def download_korean_font_safe():
    """
    Railway 환경에서 안전하고 빠른 한국어 폰트 다운로드
    """
    font_dir = Path("fonts")
    font_dir.mkdir(exist_ok=True)
    
    font_path = font_dir / "NotoSansKR-Regular.ttf"
    
    # 이미 존재하고 크기가 적절하면 재사용
    if font_path.exists() and font_path.stat().st_size > 10240:
        return str(font_path)
    
    # 검증된 한국어 폰트 URL (빠른 CDN 우선)
    font_urls = [
        "https://fonts.gstatic.com/s/notosanskr/v27/PbykFmXiEBPT4ITbgNA5Cgm20xz64px_1hVWr0wuPNGmlQNMEfD4.ttf",
        "https://cdn.jsdelivr.net/gh/fonts-archive/NotoSansKR/NotoSansKR-Regular.ttf",
    ]
    
    for i, font_url in enumerate(font_urls):
        try:
            app.logger.info(f"폰트 다운로드 시도 {i+1}: {font_url.split('/')[-1]}")
            
            req = urllib.request.Request(font_url, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; FontDownloader/1.0)'
            })
            
            # 타임아웃을 5초로 단축 (Worker 타임아웃 방지)
            with urllib.request.urlopen(req, timeout=5) as response:
                font_data = response.read()
                
            if len(font_data) > 10240:  # 최소 10KB
                font_path.write_bytes(font_data)
                app.logger.info(f"✅ 폰트 다운로드 완료: {len(font_data):,} bytes")
                return str(font_path)
            else:
                app.logger.warning(f"폰트 크기가 너무 작음: {len(font_data)} bytes")
                
        except Exception as e:
            app.logger.warning(f"폰트 다운로드 실패 ({i+1}): {str(e)[:100]}")
            font_path.unlink(missing_ok=True)
    
    app.logger.error("모든 폰트 다운로드 시도 실패")
    return None

def get_korean_font_safe(size=36):
    """
    Railway 환경에서 안전한 한국어 폰트 로드 (폴백 시스템 강화)
    """
    try:
        # 1. 시스템 폰트 우선 시도
        system_fonts = [
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            '/System/Library/Fonts/Helvetica.ttc',  # macOS
            'C:/Windows/Fonts/arial.ttf'  # Windows
        ]
        
        for font_path in system_fonts:
            if os.path.exists(font_path):
                try:
                    font = ImageFont.truetype(font_path, size)
                    app.logger.debug(f"시스템 폰트 사용: {font_path}")
                    return font
                except Exception:
                    continue
        
        # 2. 다운로드 폰트 시도 (5초 타임아웃)
        korean_font_path = download_korean_font_safe()
        if korean_font_path and os.path.exists(korean_font_path):
            try:
                font = ImageFont.truetype(korean_font_path, size)
                app.logger.debug(f"다운로드 폰트 사용: {korean_font_path}")
                return font
            except Exception as e:
                app.logger.warning(f"다운로드 폰트 로드 실패: {e}")
        
        # 3. 최종 폴백: 기본 폰트
        app.logger.info("기본 폰트 사용 (한국어 지원 제한)")
        return ImageFont.load_default()
        
    except Exception as e:
        app.logger.error(f"폰트 로드 중 심각한 오류: {e}")
        return ImageFont.load_default()

def get_text_dimensions_safe(text, font, draw):
    """안전한 텍스트 크기 계산"""
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    except Exception:
        try:
            return draw.textsize(text, font=font)
        except Exception:
            return len(text) * 12, 24  # 폴백 크기

def split_korean_text_safe(text, font, max_width, draw):
    """안전한 한국어 텍스트 분할"""
    try:
        words = text.split()
        lines = []
        current_line = ""
        
        for word in words:
            test_line = current_line + (" " if current_line else "") + word
            test_width, _ = get_text_dimensions_safe(test_line, font, draw)
            
            if test_width <= max_width:
                current_line = test_line
            else:
                if current_line:
                    lines.append(current_line)
                    current_line = word
                else:
                    # 강제 분할
                    max_chars = max(1, max_width // 12)
                    lines.append(word[:max_chars])
                    current_line = word[max_chars:] if len(word) > max_chars else ""
        
        if current_line:
            lines.append(current_line)
        
        return lines
        
    except Exception as e:
        app.logger.error(f"텍스트 분할 오류: {e}")
        max_chars = max(1, max_width // 12)
        return [text[i:i+max_chars] for i in range(0, len(text), max_chars)]

def create_qr_with_logo_safe(link_url, output_path, logo_path='static/logo.png', lecture_title=""):
    """
    안전한 QR 코드 생성 - 실패 방지 및 성능 최적화
    """
    try:
        # QR 코드 생성 (기본 설정으로 단순화)
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_M,  # 중간 수준으로 변경
            box_size=10,  # 크기 축소로 성능 향상
            border=4,
        )
        qr.add_data(link_url)
        qr.make(fit=True)
        
        qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
        qr_size = 400  # 크기 축소
        qr_img = qr_img.resize((qr_size, qr_size), Image.LANCZOS)
        
        # 로고 삽입 (선택적)
        if os.path.exists(logo_path):
            try:
                logo = Image.open(logo_path)
                logo_size = int(qr_size * 0.15)  # 로고 크기 축소
                logo = logo.resize((logo_size, logo_size), Image.LANCZOS)
                
                logo_bg = Image.new('RGB', (int(logo_size * 1.2), int(logo_size * 1.2)), 'white')
                logo_pos = ((logo_bg.size[0] - logo_size) // 2, (logo_bg.size[1] - logo_size) // 2)
                
                if logo.mode == 'RGBA':
                    logo_bg.paste(logo, logo_pos, mask=logo.split()[3])
                else:
                    logo_bg.paste(logo, logo_pos)
                
                pos = ((qr_size - logo_bg.size[0]) // 2, (qr_size - logo_bg.size[1]) // 2)
                qr_img.paste(logo_bg, pos)
                
            except Exception as e:
                app.logger.warning(f"로고 삽입 실패 (계속 진행): {e}")
        
        # 강의명 텍스트 추가 (안전 모드)
        if lecture_title and lecture_title.strip():
            try:
                text_height = 60
                margin = 15
                total_height = qr_size + text_height + margin
                final_img = Image.new('RGB', (qr_size, total_height), 'white')
                final_img.paste(qr_img, (0, 0))
                
                draw = ImageDraw.Draw(final_img)
                font = get_korean_font_safe(24)  # 폰트 크기 축소
                
                # 텍스트 길이 제한
                if len(lecture_title) > 30:
                    lecture_title = lecture_title[:30] + "..."
                
                lines = split_korean_text_safe(lecture_title, font, qr_size - 20, draw)
                
                # 최대 2줄로 제한
                lines = lines[:2]
                
                text_y_start = qr_size + margin
                for i, line in enumerate(lines):
                    if line.strip():
                        text_width, line_height = get_text_dimensions_safe(line, font, draw)
                        text_x = max(0, (qr_size - text_width) // 2)
                        text_y = text_y_start + (i * 25)
                        
                        draw.text((text_x, text_y), line, font=font, fill='black')
                
                final_img.save(output_path, quality=85, optimize=True)
                
            except Exception as text_error:
                app.logger.warning(f"텍스트 추가 실패, QR만 저장: {text_error}")
                qr_img.save(output_path, quality=85, optimize=True)
        else:
            qr_img.save(output_path, quality=85, optimize=True)
            
        app.logger.info(f"✅ QR 코드 생성 완료: {lecture_title[:20]}...")
        
    except Exception as e:
        app.logger.error(f"❌ QR 코드 생성 실패: {e}")
        # 최후 수단: 텍스트 없는 간단한 QR 코드
        try:
            simple_qr = qrcode.make(link_url)
            simple_qr.save(output_path)
            app.logger.info("✅ 간단 QR 코드로 대체")
        except Exception as final_error:
            app.logger.error(f"❌ 간단 QR 코드도 실패: {final_error}")
            raise

# ==== 나머지 기존 함수들 (URL 만료 체크 등) ====

def is_presigned_url_expired(url, safety_margin_minutes=60):
    """presigned URL 만료 여부 확인"""
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
    """week_str 형식: "YYYY-Www" 파싱"""
    try:
        year_part, week_part = week_str.split('-W')
        year = int(year_part)
        week_num = int(week_part)
        week_start_date = date.fromisocalendar(year, week_num, 1)
        week_end_date = week_start_date + timedelta(days=6)
        week_start_dt = datetime.combine(week_start_date, datetime.min.time())
        week_end_dt = datetime.combine(week_end_date, datetime.max.time())
        return week_start_dt, week_end_dt
    except Exception as e:
        raise ValueError(f"잘못된 week_str 형식: {week_str} ({e})")

# ==== JWT 관련 함수들 ====

def create_jwt_for_admin():
    """관리자 로그인 시 JWT 발급"""
    now = datetime.utcnow()
    payload = {
        'sub': ADMIN_EMAIL,
        'iat': now,
        'exp': now + timedelta(hours=JWT_EXPIRES_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

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
    """관리자 인증 데코레이터 (JWT 전용)"""
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

# ==== 🆕 수정된 관리자 인증 데코레이터 (세션과 JWT 둘 다 지원) ====
def admin_required_flexible(f):
    """유연한 관리자 인증 데코레이터 - 세션 또는 JWT 토큰 둘 중 하나만 있어도 허용"""
    @wraps(f)
    def decorated(*args, **kwargs):
        # 1. 세션 확인
        if session.get('logged_in'):
            return f(*args, **kwargs)
        
        # 2. JWT 토큰 확인
        auth_header = request.headers.get('Authorization', None)
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ', 1)[1]
            if verify_jwt_token(token):
                return f(*args, **kwargs)
        
        # 3. 둘 다 없으면 인증 실패
        return jsonify({'error': '관리자 인증이 필요합니다'}), 401
    
    return decorated

# ===================================================================
# 🆕 Flutter 호환 다국어 처리 함수들 (성능 최적화)
# ===================================================================

def get_video_with_language_support(group_id, lang_code='ko'):
    """Flutter와 호환되는 언어별 비디오 정보 조회"""
    try:
        root_doc = db.collection('uploads').document(group_id).get()
        if not root_doc.exists:
            return None
        
        root_data = root_doc.to_dict()
        
        # 기본 비디오 URL과 키 (한국어 원본)
        video_url = root_data.get('presigned_url', '')
        video_key = root_data.get('video_key', '')
        
        # 기본 정보 설정
        display_data = {
            'display_title': root_data.get('group_name', ''),
            'display_main_category': root_data.get('main_category', ''),
            'display_sub_category': root_data.get('sub_category', ''),
            'display_sub_sub_category': root_data.get('sub_sub_category', ''),
            'current_language': 'ko',
            'language_name': '한국어'
        }
        
        # 🆕 언어별 번역 문서 조회
        if lang_code != 'ko':
            translation_doc = db.collection('uploads').document(group_id) \
                               .collection('translations').document(lang_code).get()
            
            if translation_doc.exists:
                translation_data = translation_doc.to_dict()
                
                # 🎬 언어별 비디오가 있는지 확인
                if translation_data.get('video_key'):
                    video_key = translation_data['video_key']
                    video_url = translation_data.get('video_presigned_url', '')
                    
                    # URL 갱신 필요한 경우
                    if not video_url or is_presigned_url_expired(video_url, 60):
                        video_url = generate_presigned_url(video_key, expires_in=604800)
                        # 비동기로 URL 업데이트
                        translation_doc.reference.update({
                            'video_presigned_url': video_url,
                            'url_updated_at': datetime.utcnow().isoformat()
                        })
                
                # 번역된 텍스트 정보 업데이트
                display_data.update({
                    'display_title': translation_data.get('title', root_data.get('group_name')),
                    'display_main_category': translation_data.get('main_category', root_data.get('main_category')),
                    'display_sub_category': translation_data.get('sub_category', root_data.get('sub_category')),
                    'display_sub_sub_category': translation_data.get('sub_sub_category', root_data.get('sub_sub_category')),
                    'current_language': lang_code,
                    'language_name': translation_data.get('language_name', SUPPORTED_LANGUAGES.get(lang_code, lang_code))
                })
        
        # 한국어 원본 URL 갱신 확인
        if lang_code == 'ko' and (not video_url or is_presigned_url_expired(video_url, 60)):
            video_url = generate_presigned_url(video_key, expires_in=604800)
            root_doc.reference.update({
                'presigned_url': video_url,
                'updated_at': datetime.utcnow().isoformat()
            })
        
        # 최종 데이터 구성
        result = {
            **root_data,
            **display_data,
            'video_key': video_key,
            'presigned_url': video_url
        }
        
        return result
        
    except Exception as e:
        app.logger.error(f"언어별 비디오 조회 실패 ({group_id}, {lang_code}): {e}")
        return None

def get_available_languages_for_video(group_id):
    """특정 비디오에서 사용 가능한 언어 목록 조회 - Flutter 호환"""
    try:
        available = {'ko': True}  # 한국어 원본은 항상 존재
        
        # translations 컬렉션 조회
        translations_ref = db.collection('uploads').document(group_id).collection('translations')
        trans_docs = translations_ref.stream()
        
        for trans_doc in trans_docs:
            lang_code = trans_doc.id
            trans_data = trans_doc.to_dict()
            
            # video_key가 있으면 해당 언어 영상 존재
            has_video = bool(trans_data.get('video_key'))
            
            if lang_code in SUPPORTED_LANGUAGES:
                available[lang_code] = has_video
        
        # 지원하는 모든 언어에 대해 기본값 설정
        for lang_code in SUPPORTED_LANGUAGES.keys():
            if lang_code not in available:
                available[lang_code] = False
        
        # Flutter 앱 호환 형식으로 변환
        available_list = []
        for lang_code, has_video in available.items():
            if has_video:
                available_list.append({
                    'code': lang_code,
                    'name': SUPPORTED_LANGUAGES[lang_code],
                    'flag': _get_language_flag(lang_code),
                    'available': True
                })
        
        return available_list
        
    except Exception as e:
        app.logger.error(f"사용 가능한 언어 조회 실패 ({group_id}): {e}")
        return [{'code': 'ko', 'name': '한국어', 'flag': '🇰🇷', 'available': True}]

def _get_language_flag(lang_code):
    """언어 코드에 대한 국기 이모지 반환"""
    flags = {
        'ko': '🇰🇷',
        'en': '🇺🇸', 
        'zh': '🇨🇳',
        'vi': '🇻🇳',
        'th': '🇹🇭',
        'ja': '🇯🇵'
    }
    return flags.get(lang_code, '🌐')

# ===================================================================
# 🆕 관리자용 언어별 영상 관리 API (수정됨)
# ===================================================================

@app.route('/api/admin/videos', methods=['GET'])
@admin_required_flexible  # 🔧 변경됨
def get_admin_videos():
    """관리자용 영상 목록 조회 - 언어별 영상 상태 포함"""
    try:
        app.logger.info("📋 관리자 영상 목록 조회 시작")
        
        # Firestore에서 전체 영상 목록 가져오기
        uploads_ref = db.collection('uploads')
        docs = uploads_ref.order_by('created_at', direction=firestore.Query.DESCENDING).stream()
        
        videos = []
        
        for doc in docs:
            try:
                data = doc.to_dict()
                group_id = doc.id
                
                # 패키지는 제외 (일반 영상만)
                if data.get('is_package'):
                    continue
                
                # 언어별 영상 상태 확인
                languages = {'ko': True}  # 한국어 원본은 항상 존재
                
                # translations 서브컬렉션 조회
                translations_ref = doc.reference.collection('translations')
                trans_docs = translations_ref.stream()
                
                for trans_doc in trans_docs:
                    trans_data = trans_doc.to_dict()
                    lang_code = trans_doc.id
                    
                    # video_key가 있으면 해당 언어 영상이 업로드됨
                    has_video = bool(trans_data.get('video_key'))
                    languages[lang_code] = has_video
                
                # 지원하는 모든 언어에 대해 상태 설정
                for lang_code in SUPPORTED_LANGUAGES.keys():
                    if lang_code not in languages:
                        languages[lang_code] = False
                
                video_info = {
                    'group_id': group_id,
                    'title': data.get('group_name', '제목 없음'),
                    'main_category': data.get('main_category', ''),
                    'sub_category': data.get('sub_category', ''),
                    'sub_sub_category': data.get('sub_sub_category', ''),
                    'upload_date': data.get('upload_date', ''),
                    'created_at': data.get('created_at', ''),
                    'time': data.get('time', '0:00'),
                    'level': data.get('level', ''),
                    'tag': data.get('tag', ''),
                    'languages': languages,
                    'translation_status': data.get('translation_status', 'unknown')
                }
                
                videos.append(video_info)
                
            except Exception as doc_error:
                app.logger.error(f"문서 처리 오류 ({doc.id}): {doc_error}")
                continue
        
        app.logger.info(f"✅ 영상 목록 조회 완료: {len(videos)}개")
        
        return jsonify({
            'videos': videos,
            'total_count': len(videos),
            'supported_languages': SUPPORTED_LANGUAGES
        }), 200
        
    except Exception as e:
        app.logger.error(f"❌ 영상 목록 조회 실패: {e}")
        return jsonify({'error': '영상 목록을 가져올 수 없습니다'}), 500

@app.route('/api/admin/upload_language_video', methods=['POST'])
@admin_required_flexible  # 🔧 변경됨
def upload_language_video():
    """언어별 영상 업로드 - 개선된 버전"""
    try:
        file = request.files.get('file')
        group_id = request.form.get('group_id', '').strip()
        language_code = request.form.get('language_code', '').strip()
        
        # 입력 검증
        if not file or not group_id or not language_code:
            return jsonify({'error': '파일, group_id, language_code가 모두 필요합니다'}), 400
            
        if language_code not in SUPPORTED_LANGUAGES or language_code == 'ko':
            return jsonify({'error': f'지원하지 않는 언어 코드입니다: {language_code}'}), 400
        
        if not file.filename:
            return jsonify({'error': '유효한 파일을 선택해주세요'}), 400
        
        # 파일 크기 확인 (200MB 제한)
        file.seek(0, 2)  # 파일 끝으로 이동
        file_size = file.tell()
        file.seek(0)  # 파일 처음으로 복원
        
        if file_size > 200 * 1024 * 1024:  # 200MB
            return jsonify({'error': '파일 크기는 200MB를 초과할 수 없습니다'}), 400
        
        app.logger.info(f"🌐 언어별 영상 업로드 시작: {group_id} - {language_code} ({file_size:,} bytes)")
        
        # 원본 문서 확인
        root_doc = db.collection('uploads').document(group_id).get()
        if not root_doc.exists:
            return jsonify({'error': '원본 영상을 찾을 수 없습니다'}), 404
            
        root_data = root_doc.to_dict()
        
        # 이미 해당 언어 영상이 있는지 확인
        trans_doc = db.collection('uploads').document(group_id) \
                     .collection('translations').document(language_code).get()
        
        if trans_doc.exists and trans_doc.to_dict().get('video_key'):
            return jsonify({'error': f'{SUPPORTED_LANGUAGES[language_code]} 영상이 이미 업로드되어 있습니다'}), 409
        
        # 파일 이름에서 언어별 키 생성
        original_video_key = root_data.get('video_key', '')
        if not original_video_key:
            return jsonify({'error': '원본 영상 키를 찾을 수 없습니다'}), 400
            
        folder = '/'.join(original_video_key.split('/')[:-1])  # 폴더 경로 추출
        ext = Path(file.filename).suffix.lower() or '.mp4'
        
        # 언어별 비디오 키 생성
        language_video_key = f"{folder}/video_{language_code}{ext}"
        
        # 임시 파일로 저장
        tmp_path = Path(tempfile.gettempdir()) / f"{group_id}_{language_code}_{uuid.uuid4().hex[:8]}{ext}"
        
        try:
            file.save(str(tmp_path))
            app.logger.info(f"임시 파일 저장: {tmp_path}")
            
            # S3 업로드
            s3.upload_file(str(tmp_path), BUCKET_NAME, language_video_key, Config=config)
            app.logger.info(f"S3 업로드 완료: {language_video_key}")
            
            # Presigned URL 생성
            presigned_url = generate_presigned_url(language_video_key, expires_in=604800)
            
            # 영상 길이 계산 (선택적)
            video_duration = None
            try:
                with VideoFileClip(str(tmp_path)) as clip:
                    duration_sec = int(clip.duration)
                    minutes = duration_sec // 60
                    seconds = duration_sec % 60
                    video_duration = f"{minutes}:{seconds:02d}"
            except Exception as duration_error:
                app.logger.warning(f"영상 길이 계산 실패: {duration_error}")
            
            # translations 컬렉션 업데이트
            translation_ref = db.collection('uploads').document(group_id) \
                               .collection('translations').document(language_code)
            
            update_data = {
                'video_key': language_video_key,
                'video_presigned_url': presigned_url,
                'video_uploaded_at': datetime.utcnow().isoformat(),
                'video_file_size': file_size,
                'video_file_name': file.filename,
                'language_code': language_code,
                'language_name': SUPPORTED_LANGUAGES[language_code],
                'is_original': False
            }
            
            if video_duration:
                update_data['video_duration'] = video_duration
            
            # 기존 번역 데이터가 있는지 확인
            if trans_doc.exists:
                # 기존 데이터에 video 정보만 추가
                translation_ref.update(update_data)
            else:
                # 새로운 번역 문서 생성 (텍스트 번역도 포함)
                update_data.update({
                    'title': translate_text_safe(root_data.get('group_name', ''), language_code),
                    'main_category': translate_text_safe(root_data.get('main_category', ''), language_code),
                    'sub_category': translate_text_safe(root_data.get('sub_category', ''), language_code),
                    'sub_sub_category': translate_text_safe(root_data.get('sub_sub_category', ''), language_code),
                    'translated_at': datetime.utcnow().isoformat()
                })
                translation_ref.set(update_data)
            
            # 루트 문서 업데이트 (언어별 영상 추가됨을 표시)
            root_doc.reference.update({
                'has_language_videos': True,
                'last_language_upload': datetime.utcnow().isoformat(),
                f'lang_{language_code}_video': True
            })
            
            app.logger.info(f"✅ 언어별 영상 업로드 완료: {group_id} - {language_code}")
            
            return jsonify({
                'success': True,
                'message': f'{SUPPORTED_LANGUAGES[language_code]} 영상이 성공적으로 업로드되었습니다.',
                'group_id': group_id,
                'language': language_code,
                'language_name': SUPPORTED_LANGUAGES[language_code],
                'video_key': language_video_key,
                'video_url': presigned_url,
                'video_duration': video_duration,
                'file_size_mb': round(file_size / (1024 * 1024), 2)
            }), 200
            
        finally:
            # 임시 파일 정리
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            
    except Exception as e:
        app.logger.error(f"❌ 언어별 영상 업로드 실패: {e}")
        return jsonify({
            'success': False,
            'error': f'업로드 중 오류가 발생했습니다: {str(e)}'
        }), 500

# ==== 🆕 관리자 영상 삭제 API (추가) ====
@app.route('/api/admin/delete_language_video', methods=['DELETE'])
@admin_required_flexible
def delete_language_video():
    """언어별 영상 삭제"""
    try:
        data = request.get_json() or {}
        group_id = data.get('group_id', '').strip()
        language_code = data.get('language_code', '').strip()
        
        if not group_id or not language_code:
            return jsonify({'error': 'group_id와 language_code가 필요합니다'}), 400
            
        if language_code == 'ko':
            return jsonify({'error': '한국어 원본 영상은 삭제할 수 없습니다'}), 400
            
        # 번역 문서 확인
        trans_ref = db.collection('uploads').document(group_id) \
                     .collection('translations').document(language_code)
        trans_doc = trans_ref.get()
        
        if not trans_doc.exists:
            return jsonify({'error': '해당 언어의 영상을 찾을 수 없습니다'}), 404
            
        trans_data = trans_doc.to_dict()
        video_key = trans_data.get('video_key')
        
        if not video_key:
            return jsonify({'error': '영상 파일이 없습니다'}), 404
        
        # S3에서 삭제
        try:
            s3.delete_object(Bucket=BUCKET_NAME, Key=video_key)
            app.logger.info(f"S3 영상 삭제 완료: {video_key}")
        except Exception as s3_error:
            app.logger.error(f"S3 삭제 실패: {s3_error}")
        
        # Firestore 업데이트 (영상 정보만 제거, 번역 텍스트는 유지)
        trans_ref.update({
            'video_key': firestore.DELETE,
            'video_presigned_url': firestore.DELETE,
            'video_uploaded_at': firestore.DELETE,
            'video_file_size': firestore.DELETE,
            'video_file_name': firestore.DELETE,
            'video_duration': firestore.DELETE
        })
        
        # 루트 문서 업데이트
        root_ref = db.collection('uploads').document(group_id)
        root_ref.update({
            f'lang_{language_code}_video': False
        })
        
        app.logger.info(f"✅ 언어별 영상 삭제 완료: {group_id} - {language_code}")
        
        return jsonify({
            'success': True,
            'message': f'{SUPPORTED_LANGUAGES[language_code]} 영상이 삭제되었습니다.',
            'group_id': group_id,
            'language': language_code
        }), 200
        
    except Exception as e:
        app.logger.error(f"영상 삭제 실패: {e}")
        return jsonify({
            'success': False,
            'error': f'삭제 중 오류가 발생했습니다: {str(e)}'
        }), 500

# ==== 🆕 세션 상태 확인 API (추가) ====
@app.route('/api/admin/check_auth', methods=['GET'])
def check_admin_auth():
    """관리자 인증 상태 확인"""
    session_auth = session.get('logged_in', False)
    
    auth_header = request.headers.get('Authorization', '')
    token_auth = False
    if auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
        token_auth = verify_jwt_token(token)
    
    return jsonify({
        'authenticated': session_auth or token_auth,
        'session_auth': session_auth,
        'token_auth': token_auth,
        'message': '인증됨' if (session_auth or token_auth) else '인증 필요'
    })

# ===================================================================
# 🆕 Flutter 호환 비디오 시청 API
# ===================================================================

@app.route('/watch/<group_id>', methods=['GET'])
def watch(group_id):
    """동영상 시청 페이지 - Flutter 완전 호환 (언어별 영상 지원)"""
    try:
        requested_lang = request.args.get('lang', 'ko')
        
        if requested_lang not in SUPPORTED_LANGUAGES:
            requested_lang = 'ko'
        
        user_agent = request.headers.get('User-Agent', '').lower()
        is_flutter_app = 'flutter' in user_agent or 'dart' in user_agent
        
        # 🆕 언어별 비디오 정보 가져오기 (개선된 버전)
        video_data = get_video_with_language_support(group_id, requested_lang)
        if not video_data:
            if is_flutter_app:
                return jsonify({'error': 'Video not found'}), 404
            else:
                abort(404)
        
        # 🆕 사용 가능한 언어 목록 추가
        available_languages = get_available_languages_for_video(group_id)
        
        if is_flutter_app:
            # Flutter 앱용 응답 (video_warning_screen.dart 호환)
            return jsonify({
                'groupId': group_id,
                'videoId': group_id,  # Flutter 호환성
                'title': video_data['display_title'],
                'group_name': video_data['display_title'],  # Flutter 호환성
                'main_category': video_data['display_main_category'],
                'sub_category': video_data['display_sub_category'],
                'sub_sub_category': video_data['display_sub_sub_category'],
                'video_url': video_data['presigned_url'],
                'videoUrl': video_data['presigned_url'],  # Flutter 호환성
                'qr_url': video_data.get('qr_presigned_url', ''),
                'thumbnail_url': video_data.get('thumbnail_presigned_url', ''),
                'language': requested_lang,
                'selectedLanguage': requested_lang,  # Flutter 호환성
                'language_name': video_data.get('language_name', '한국어'),
                'time': video_data.get('time', '0:00'),
                'level': video_data.get('level', ''),
                'tag': video_data.get('tag', ''),
                'available_languages': available_languages,
                'has_language_videos': video_data.get('has_language_videos', False),
                'supported_languages': SUPPORTED_LANGUAGES,  # Flutter 호환성
                'success': True
            })
        else:
            return render_template(
                'watch.html',
                video_url=video_data['presigned_url'],
                video_data=video_data,
                available_languages=available_languages,
                supported_languages=SUPPORTED_LANGUAGES,
                current_language=requested_lang
            )
            
    except Exception as e:
        app.logger.error(f"시청 페이지 오류: {e}")
        if 'is_flutter_app' in locals() and is_flutter_app:
            return jsonify({'error': '비디오 로드 중 오류 발생', 'success': False}), 500
        else:
            abort(500)

# ===================================================================
# 🆕 언어별 영상 URL 조회 API (Flutter video_warning_screen.dart 호환)
# ===================================================================

@app.route('/api/video/<group_id>/language/<lang_code>', methods=['GET'])
def get_language_video_url(group_id, lang_code):
    """특정 언어의 영상 URL 조회 - Flutter 호환"""
    try:
        if lang_code not in SUPPORTED_LANGUAGES:
            return jsonify({'error': '지원하지 않는 언어입니다', 'success': False}), 400
        
        # 언어별 영상 정보 조회
        video_data = get_video_with_language_support(group_id, lang_code)
        if not video_data:
            return jsonify({'error': '비디오를 찾을 수 없습니다', 'success': False}), 404
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'language': lang_code,
            'language_name': SUPPORTED_LANGUAGES[lang_code],
            'video_url': video_data['presigned_url'],
            'title': video_data['display_title'],
            'has_language_video': bool(video_data.get('video_key')),
            'available_languages': get_available_languages_for_video(group_id)
        }), 200
        
    except Exception as e:
        app.logger.error(f"언어별 영상 URL 조회 오류: {e}")
        return jsonify({'error': '영상 정보를 가져올 수 없습니다', 'success': False}), 500

@app.route('/api/video/<group_id>/languages', methods=['GET'])
def get_video_languages(group_id):
    """특정 비디오의 사용 가능한 언어 목록 API - Flutter 호환"""
    try:
        available_languages = get_available_languages_for_video(group_id)
        
        return jsonify({
            'success': True,
            'group_id': group_id,
            'available_languages': available_languages,
            'total_languages': len(available_languages),
            'supported_languages': SUPPORTED_LANGUAGES
        }), 200
        
    except Exception as e:
        app.logger.error(f"언어 목록 API 오류: {e}")
        return jsonify({'error': '언어 목록을 가져올 수 없습니다', 'success': False}), 500

# ===================================================================
# 백그라운드 자동 갱신 시스템 (언어별 영상 포함)
# ===================================================================

def refresh_expiring_urls():
    """만료 임박한 presigned URL들을 일괄 갱신 - 언어별 영상도 포함"""
    try:
        app.logger.info("🔄 백그라운드 URL 갱신 작업 시작...")
        
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
            
            # 루트 문서 URL 갱신
            if not current_url or is_presigned_url_expired(current_url, safety_margin_minutes=120):
                try:
                    new_presigned_url = generate_presigned_url(video_key, expires_in=604800)
                    
                    update_data = {
                        'presigned_url': new_presigned_url,
                        'auto_updated_at': datetime.utcnow().isoformat(),
                        'auto_update_reason': 'background_refresh'
                    }
                    
                    qr_key = data.get('qr_key', '')
                    if qr_key:
                        new_qr_url = generate_presigned_url(qr_key, expires_in=604800)
                        update_data['qr_presigned_url'] = new_qr_url
                    
                    thumbnail_key = data.get('thumbnail_key', '')
                    if thumbnail_key:
                        new_thumbnail_url = generate_presigned_url(thumbnail_key, expires_in=604800)
                        update_data['thumbnail_presigned_url'] = new_thumbnail_url
                    
                    doc.reference.update(update_data)
                    updated_count += 1
                    
                except Exception as update_error:
                    app.logger.error(f"URL 갱신 실패 {doc.id}: {update_error}")
            
            # 🆕 언어별 영상 URL도 갱신
            try:
                translations = doc.reference.collection('translations').stream()
                for trans_doc in translations:
                    trans_data = trans_doc.to_dict()
                    if trans_data.get('video_key'):
                        trans_url = trans_data.get('video_presigned_url', '')
                        if not trans_url or is_presigned_url_expired(trans_url, safety_margin_minutes=120):
                            new_trans_url = generate_presigned_url(trans_data['video_key'], expires_in=604800)
                            trans_doc.reference.update({
                                'video_presigned_url': new_trans_url,
                                'url_updated_at': datetime.utcnow().isoformat()
                            })
                            updated_count += 1
            except Exception as trans_error:
                app.logger.error(f"번역 URL 갱신 실패 {doc.id}: {trans_error}")
        
        app.logger.info(f"🎉 백그라운드 URL 갱신 완료: {updated_count}/{total_count}")
        
    except Exception as e:
        app.logger.error(f"❌ 백그라운드 URL 갱신 오류: {e}")

# ===================================================================
# 스케줄러 설정
# ===================================================================

scheduler = BackgroundScheduler(
    timezone='UTC',
    job_defaults={
        'coalesce': True,
        'max_instances': 1
    }
)

def start_background_scheduler():
    """백그라운드 스케줄러 시작"""
    try:
        scheduler.add_job(
            func=refresh_expiring_urls,
            trigger=IntervalTrigger(hours=6),  # 6시간으로 변경 (부하 감소)
            id='refresh_video_urls',
            name='동영상 URL 자동 갱신',
            replace_existing=True
        )
        
        scheduler.start()
        app.logger.info("🚀 백그라운드 스케줄러 시작 (6시간 간격)")
        atexit.register(lambda: scheduler.shutdown())
        
    except Exception as e:
        app.logger.error(f"❌ 스케줄러 시작 실패: {e}")

# ===================================================================
# 업로드 핸들러 (성능 최적화) - 기존 코드 유지
# ===================================================================

@app.route('/upload', methods=['POST'])
def upload_video():
    """최적화된 업로드 처리 - 번역을 백그라운드로 이동"""
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    file = request.files.get('file')
    thumbnail = request.files.get('thumbnail')
    group_name = request.form.get('group_name', 'default')
    main_cat = request.form.get('main_category', '')
    sub_cat = request.form.get('sub_category', '')
    leaf_cat = request.form.get('sub_sub_category', '')
    lecture_level = request.form.get('level', '')
    lecture_tag = request.form.get('tag', '')

    if not file:
        return "파일이 필요합니다.", 400

    # 🚀 1) 즉시 번역 (한국어 + 영어만, 나머지는 백그라운드)
    app.logger.info(f"즉시 번역 시작: '{group_name}'")
    immediate_translations = {
        'ko': group_name,
        'en': translate_text_safe(group_name, 'en')
    }

    # 2) 그룹 ID 생성 및 S3 키 구성
    group_id = uuid.uuid4().hex
    date_str = datetime.now().strftime('%Y%m%d')
    safe_name = re.sub(r'[^\w]', '_', group_name)
    folder = f"videos/{group_id}_{safe_name}_{date_str}"
    
    ext = Path(file.filename).suffix.lower() or '.mp4'
    video_key = f"{folder}/video{ext}"

    # 3) 임시 저장 및 S3 업로드
    tmp_path = Path(tempfile.gettempdir()) / f"{group_id}{ext}"
    file.save(tmp_path)

    # 4) 동영상 길이 계산
    try:
        with VideoFileClip(str(tmp_path)) as clip:
            duration_sec = int(clip.duration)
    except Exception as e:
        duration_sec = 0
        app.logger.warning(f"동영상 길이 계산 실패: {e}")

    minutes = duration_sec // 60
    seconds = duration_sec % 60
    lecture_time = f"{minutes}:{seconds:02d}"

    # S3 업로드
    s3.upload_file(str(tmp_path), BUCKET_NAME, video_key, Config=config)
    tmp_path.unlink(missing_ok=True)

    presigned_url = generate_presigned_url(video_key, expires_in=604800)

    # 5) 썸네일 처리
    thumbnail_key = None
    thumbnail_presigned_url = None
    if thumbnail and thumbnail.filename:
        try:
            thumb_ext = Path(thumbnail.filename).suffix.lower() or '.jpg'
            thumbnail_key = f"{folder}/thumbnail{thumb_ext}"
            
            thumb_tmp_path = Path(tempfile.gettempdir()) / f"{group_id}_thumb{thumb_ext}"
            thumbnail.save(thumb_tmp_path)
            
            s3.upload_file(str(thumb_tmp_path), BUCKET_NAME, thumbnail_key, Config=config)
            thumb_tmp_path.unlink(missing_ok=True)
            
            thumbnail_presigned_url = generate_presigned_url(thumbnail_key, expires_in=604800)
            
        except Exception as e:
            app.logger.error(f"썸네일 업로드 실패: {e}")

    # 6) QR 코드 생성 (안전 모드)
    qr_link = f"{APP_BASE_URL}{group_id}"
    qr_filename = f"{uuid.uuid4().hex}.png"
    local_qr = os.path.join(app.config['UPLOAD_FOLDER'], qr_filename)
    
    display_title = group_name
    if main_cat or sub_cat or leaf_cat:
        categories = [cat for cat in [main_cat, sub_cat, leaf_cat] if cat]
        if categories:
            display_title = f"{group_name}\n({' > '.join(categories)})"
    
    create_qr_with_logo_safe(qr_link, local_qr, lecture_title=display_title)
    
    qr_key = f"{folder}/{qr_filename}"
    s3.upload_file(local_qr, BUCKET_NAME, qr_key)
    qr_presigned_url = generate_presigned_url(qr_key, expires_in=604800)
    
    try:
        os.remove(local_qr)
    except OSError:
        pass

    # 7) 루트 문서 저장
    root_doc_data = {
        'group_id': group_id,
        'group_name': group_name,
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
        'updated_at': datetime.utcnow().isoformat(),
        'translation_status': 'partial'  # 부분 번역 상태
    }

    if thumbnail_key:
        root_doc_data['thumbnail_key'] = thumbnail_key
        root_doc_data['thumbnail_presigned_url'] = thumbnail_presigned_url

    root_doc_ref = db.collection('uploads').document(group_id)
    root_doc_ref.set(root_doc_data)

    # 8) 즉시 번역 저장 (한국어, 영어)
    translations_ref = root_doc_ref.collection('translations')
    
    for lang_code in ['ko', 'en']:
        translation_data = {
            'title': immediate_translations[lang_code],
            'main_category': main_cat if lang_code == 'ko' else translate_text_safe(main_cat, lang_code),
            'sub_category': sub_cat if lang_code == 'ko' else translate_text_safe(sub_cat, lang_code),
            'sub_sub_category': leaf_cat if lang_code == 'ko' else translate_text_safe(leaf_cat, lang_code),
            'language_code': lang_code,
            'language_name': SUPPORTED_LANGUAGES[lang_code],
            'is_original': (lang_code == 'ko'),
            'translated_at': datetime.utcnow().isoformat()
        }
        
        translations_ref.document(lang_code).set(translation_data)

    # 🚀 9) 나머지 언어 번역을 백그라운드로 스케줄링
    def background_translate():
        remaining_languages = [lang for lang in SUPPORTED_LANGUAGES.keys() if lang not in ['ko', 'en']]
        
        for lang_code in remaining_languages:
            try:
                translation_data = {
                    'title': translate_text_safe(group_name, lang_code),
                    'main_category': translate_text_safe(main_cat, lang_code),
                    'sub_category': translate_text_safe(sub_cat, lang_code),
                    'sub_sub_category': translate_text_safe(leaf_cat, lang_code),
                    'language_code': lang_code,
                    'language_name': SUPPORTED_LANGUAGES[lang_code],
                    'is_original': False,
                    'translated_at': datetime.utcnow().isoformat()
                }
                
                translations_ref.document(lang_code).set(translation_data)
                time.sleep(0.5)  # API 호출 간격
                
            except Exception as e:
                app.logger.error(f"백그라운드 번역 실패 ({lang_code}): {e}")
        
        # 번역 완료 상태 업데이트
        root_doc_ref.update({'translation_status': 'complete'})
        app.logger.info(f"✅ 백그라운드 번역 완료: {group_id}")

    # 백그라운드 스레드로 실행
    threading.Thread(target=background_translate, daemon=True).start()

    app.logger.info(f"✅ 업로드 완료 (즉시 응답): {group_id}")

    return render_template(
        'success.html',
        group_id=group_id,
        translations=immediate_translations,
        time=lecture_time,
        level=lecture_level,
        tag=lecture_tag,
        presigned_url=presigned_url,
        qr_url=qr_presigned_url,
        thumbnail_url=thumbnail_presigned_url
    )

# ===================================================================
# 나머지 라우팅 및 API 엔드포인트들 (Flutter 호환성 강화)
# ===================================================================

@app.route('/', methods=['GET'])
def login_page():
    """로그인 페이지"""
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    """관리자 로그인"""
    try:
        pw = request.form.get('password', '')
        email = request.form.get('email', '')

        if email == ADMIN_EMAIL and pw == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('upload_form'))
        return render_template('login.html', error="인증 실패")
    except Exception as e:
        app.logger.error(f"로그인 오류: {e}")
        return render_template('login.html', error="로그인 처리 중 오류 발생")

@app.route('/api/admin/login', methods=['POST'])
def api_admin_login():
    """Flutter 관리자 로그인"""
    try:
        data = request.get_json() or {}
        email = data.get('email', '').strip()
        password = data.get('password', '')

        if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
            token = create_jwt_for_admin()
            return jsonify({'token': token, 'success': True}), 200
        else:
            return jsonify({'error': '관리자 인증 실패', 'success': False}), 401
    except Exception as e:
        app.logger.error(f"API 로그인 오류: {e}")
        return jsonify({'error': '로그인 처리 중 오류 발생', 'success': False}), 500

@app.route('/upload_form', methods=['GET'])
def upload_form():
    """업로드 폼 페이지"""
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))

    main_cats = ['기계', '공구', '장비', '약품']
    sub_map = {
        '기계': ['공작기계', '제조기계', '산업기계'],
        '공구': ['수공구', '전동공구', '절삭공구'],
        '장비': ['안전장비', '운송장비', '작업장비'],
        '약품': ['의약품', '화공약품'],
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
        '작업장비': ['스캐폴딩', '작업대', '리프트 테이블'],
        '의약품': ['항생제', '인슐린', '항응고제'],
        '화공약품': ['황산', '염산', '수산화나트륨']
    }
    return render_template('upload_form.html', mains=main_cats, subs=sub_map, leafs=leaf_map)

# ===================================================================
# 🆕 Flutter 전용 진행도 관리 API (video_playback_screen.dart 호환)
# ===================================================================

@app.route('/api/user/progress/<user_id>/<video_id>', methods=['GET'])
def get_user_progress(user_id, video_id):
    """사용자 진행도 조회 - Flutter 호환"""
    try:
        # Firebase Auth 사용자 확인
        user_doc = db.collection('users').document(user_id).get()
        if not user_doc.exists:
            return jsonify({
                'success': False,
                'progress': 0.0,
                'watchedDuration': 0,
                'lastWatchedLanguage': 'ko'
            }), 200  # 사용자가 없어도 성공으로 처리
        
        user_data = user_doc.to_dict()
        progress_map = user_data.get('progress', {})
        watched_map = user_data.get('watchedDuration', {})
        language_map = user_data.get('lastWatchedLanguage', {})
        
        return jsonify({
            'success': True,
            'progress': progress_map.get(video_id, 0.0),
            'watchedDuration': watched_map.get(video_id, 0),
            'lastWatchedLanguage': language_map.get(video_id, 'ko')
        }), 200
        
    except Exception as e:
        app.logger.error(f"진행도 조회 오류: {e}")
        return jsonify({
            'success': False,
            'error': '진행도를 가져올 수 없습니다',
            'progress': 0.0,
            'watchedDuration': 0,
            'lastWatchedLanguage': 'ko'
        }), 500

@app.route('/api/user/progress', methods=['POST'])
def save_user_progress():
    """사용자 진행도 저장 - Flutter 호환"""
    try:
        data = request.get_json() or {}
        user_id = data.get('userId') or data.get('user_id')
        video_id = data.get('videoId') or data.get('video_id')
        progress = data.get('progress', 0.0)
        watched_duration = data.get('watchedDuration', 0)
        language = data.get('language', 'ko')
        is_completed = data.get('isCompleted', False)
        lecture_title = data.get('lectureTitle', '')
        
        if not user_id or not video_id:
            return jsonify({
                'success': False,
                'error': 'userId와 videoId가 필요합니다'
            }), 400
        
        # 진행도 데이터 구성
        update_data = {
            f'progress.{video_id}': 1.0 if is_completed else progress,
            f'watchedDuration.{video_id}': watched_duration,
            f'lastWatched.{video_id}': firestore.SERVER_TIMESTAMP,
            f'lastWatchedLanguage.{video_id}': language
        }
        
        # 완료 처리
        if is_completed:
            update_data[f'completedLanguage.{video_id}'] = language
            update_data['completedVideos'] = firestore.ArrayUnion([video_id])
            
            if lecture_title:
                update_data['lastSelectedLectureId'] = video_id
                update_data['lastSelectedLecture'] = lecture_title
        
        # Firestore 업데이트
        user_ref = db.collection('users').document(user_id)
        user_ref.update(update_data)
        
        app.logger.info(f"✅ 진행도 저장 완료: {user_id} - {video_id} ({language}) - {progress*100:.1f}%")
        
        return jsonify({
            'success': True,
            'message': '진행도가 저장되었습니다',
            'saved_progress': progress,
            'saved_language': language
        }), 200
        
    except Exception as save_error:
        app.logger.error(f"진행도 저장 실패: {save_error}")
        
        # 새 문서로 재시도
        try:
            user_ref = db.collection('users').document(user_id)
            new_data = {
                'progress': {video_id: 1.0 if is_completed else progress},
                'watchedDuration': {video_id: watched_duration},
                'lastWatched': {video_id: firestore.SERVER_TIMESTAMP},
                'lastWatchedLanguage': {video_id: language}
            }
            
            if is_completed:
                new_data['completedVideos'] = [video_id]
                new_data['completedLanguage'] = {video_id: language}
                if lecture_title:
                    new_data['lastSelectedLectureId'] = video_id
                    new_data['lastSelectedLecture'] = lecture_title
            
            user_ref.set(new_data, merge=True)
            
            app.logger.info(f"✅ 새 문서로 진행도 저장 완료: {user_id} - {video_id}")
            
            return jsonify({
                'success': True,
                'message': '새 문서로 진행도가 저장되었습니다',
                'saved_progress': progress,
                'saved_language': language
            }), 200
            
        except Exception as retry_error:
            app.logger.error(f"진행도 저장 재시도도 실패: {retry_error}")
            return jsonify({
                'success': False,
                'error': '진행도 저장에 실패했습니다'
            }), 500

# ===================================================================
# 수료증 관련 API (Flutter 호환성 강화)
# ===================================================================

@app.route('/create_certificate', methods=['POST'])
def create_certificate():
    """수료증 발급 - Flutter 호환"""
    try:
        data = request.get_json() or {}
        user_uid = data.get('user_uid') or data.get('userId')
        cert_id = data.get('cert_id') or data.get('certId')
        lecture_title = data.get('lectureTitle', '')
        pdf_url = data.get('pdfUrl', '')
        language = data.get('language', 'ko')  # 🆕 언어 정보 추가

        if not user_uid or not cert_id or not pdf_url:
            return jsonify({
                'success': False,
                'error': 'user_uid, cert_id, lectureTitle, pdfUrl이 필요합니다.'
            }), 400

        cert_ref = db.collection('users').document(user_uid) \
                     .collection('completedCertificates').document(cert_id)
        
        cert_data = {
            'lectureTitle': lecture_title,
            'issuedAt': firestore.SERVER_TIMESTAMP,
            'pdfUrl': pdf_url,
            'excelUpdated': False,
            'readyForExcel': True,
            'language': language,  # 🆕 수료증 발급 언어
            'createdAt': firestore.SERVER_TIMESTAMP
        }
        
        cert_ref.set(cert_data, merge=True)

        app.logger.info(f"✅ 수료증 발급 완료: {user_uid} - {cert_id} ({language})")

        return jsonify({
            'success': True,
            'message': '수료증이 생성되었습니다.',
            'cert_id': cert_id,
            'language': language
        }), 200
        
    except Exception as e:
        app.logger.error(f"수료증 생성 오류: {e}")
        return jsonify({
            'success': False,
            'error': '수료증 생성 중 오류 발생'
        }), 500

# ===================================================================
# 헬스체크 및 관리 API (Flutter 호환성 강화)
# ===================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """서비스 상태 확인 - Flutter 호환"""
    try:
        # Firestore 연결 확인
        try:
            db.collection('uploads').limit(1).get()
            firestore_status = 'healthy'
        except Exception:
            firestore_status = 'unhealthy'
        
        # S3 연결 확인
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            s3_status = 'healthy'
        except Exception:
            s3_status = 'unhealthy'
        
        overall_status = 'healthy' if (firestore_status == 'healthy' and s3_status == 'healthy') else 'unhealthy'
        
        return jsonify({
            'success': True,
            'status': overall_status,
            'timestamp': datetime.utcnow().isoformat(),
            'services': {
                'firestore': firestore_status,
                's3': s3_status,
                'scheduler': scheduler.running if 'scheduler' in globals() else False,
                'translator': get_translator() is not None
            },
            'supported_languages': SUPPORTED_LANGUAGES,
            'language_count': len(SUPPORTED_LANGUAGES),
            'features': {
                'multilingual_video_support': True,
                'automatic_translation': True,
                'real_time_progress_tracking': True,
                'flutter_compatibility': True
            },
            'version': '2.4.0-flutter-compat'  # 버전 업데이트
        }), 200 if overall_status == 'healthy' else 503
        
    except Exception as e:
        app.logger.error(f"헬스체크 오류: {e}")
        return jsonify({
            'success': False,
            'status': 'error', 
            'message': '헬스체크 실패'
        }), 500

@app.route('/api/admin/stats', methods=['GET'])
@admin_required_flexible  # 🔧 변경됨
def get_admin_stats():
    """관리자용 통계"""
    try:
        total_videos = len(list(db.collection('uploads').stream()))
        
        return jsonify({
            'success': True,
            'total_videos': total_videos,
            'supported_languages': len(SUPPORTED_LANGUAGES),
            'language_list': list(SUPPORTED_LANGUAGES.keys()),
            'features': {
                'multilingual_video_support': True,
                'automatic_translation': True,
                'flutter_compatibility': True
            },
            'translation_cache_size': len(translation_cache),
            'scheduler_running': scheduler.running if 'scheduler' in globals() else False
        }), 200
        
    except Exception as e:
        app.logger.error(f"통계 조회 실패: {e}")
        return jsonify({
            'success': False,
            'error': '통계를 가져올 수 없습니다.'
        }), 500

# ===================================================================
# 🆕 OPTIONS 핸들러 (CORS 지원)
# ===================================================================

@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    """CORS preflight 요청 처리"""
    return '', 200

# ===================================================================
# Railway 환경 초기화 및 시작
# ===================================================================

def initialize_railway_environment():
    """Railway 배포 환경 초기화"""
    try:
        os.makedirs('static', exist_ok=True)
        os.makedirs('fonts', exist_ok=True)
        
        # 번역기 초기화
        get_translator()
        
        # 환경별 로그 레벨 설정
        if os.environ.get('RAILWAY_ENVIRONMENT'):
            import logging
            app.logger.setLevel(logging.INFO)
        
        app.logger.info("🚂 Railway 환경 초기화 완료 (Flutter 완전 호환)")
        return True
        
    except Exception as e:
        app.logger.error(f"❌ Railway 환경 초기화 실패: {e}")
        return False

if __name__ == "__main__":
    # 환경 초기화
    initialize_railway_environment()
    
    # 스케줄러 시작
    start_background_scheduler()
    
    port = int(os.environ.get("PORT", 8080))
    
    app.logger.info(f"🚀 Flutter 호환 다국어 영상 플랫폼 시작")
    app.logger.info(f"📱 지원 언어: {', '.join(SUPPORTED_LANGUAGES.values())}")
    app.logger.info(f"🌐 언어별 영상 업로드 지원")
    app.logger.info(f"🎬 실시간 진행도 추적")
    app.logger.info(f"🔒 관리자 인증 개선: 세션 + JWT 동시 지원")
    
    if os.environ.get('RAILWAY_ENVIRONMENT'):
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        app.run(host="0.0.0.0", port=port, debug=True)