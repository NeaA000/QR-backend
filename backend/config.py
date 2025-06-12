# backend/config.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pathlib import Path

# 환경변수 설정
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', '')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'changeme')
JWT_SECRET = os.environ.get('JWT_SECRET', 'supersecretjwt')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRES_HOURS = 4

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
REGION_NAME = os.environ['REGION_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'http://localhost:5000/watch/')
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'supersecret')

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

# 지원 언어
SUPPORTED_LANGUAGES = {
    'ko': '한국어',
    'en': 'English',
    'zh-cn': '中文',
    'vi': 'Tiếng Việt',
    'th': 'ไทย',
    'uz': 'O\'zbek',
    'ja': '日本語'
}

# 업로드 설정
UPLOAD_FOLDER = 'static'
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB
ALLOWED_VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm', '.m4v', '.mpg', '.mpeg'}
ALLOWED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}

# 디렉토리 생성
Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path("fonts").mkdir(exist_ok=True)
