# backend/__init__.py
"""
Backend Package for Video Learning Management System

이 패키지는 비디오 학습 관리 시스템의 백엔드 기능을 제공합니다.

주요 모듈:
- app: Flask 메인 애플리케이션
- config: 설정 관리
- auth: 인증 및 권한 관리
- database: Firestore 데이터베이스 연동
- storage: S3/Firebase Storage 파일 관리
- translation: 다국어 번역 서비스
- video_handler: 비디오 업로드 및 처리
- qr_generator: QR 코드 생성
- certificate: 수료증 관리
- scheduler: 백그라운드 작업 스케줄링
- utils: 공통 유틸리티 함수
- api_routes: REST API 엔드포인트
- web_routes: 웹 페이지 라우트
"""

# 패키지 정보
__version__ = "3.0.0"
__author__ = "Your Team"
__description__ = "Video Learning Management System Backend"

# 주요 모듈들을 패키지 레벨에서 import 가능하게 설정
# (선택사항: 필요한 경우에만 사용)

# Flask 앱 인스턴스 (필요시)
# from .app import app

# 주요 설정 (필요시)
# from .config import SUPPORTED_LANGUAGES

# 주요 유틸리티 함수들 (필요시)
# from .utils import get_video_with_translation


#config.py: 모든 설정 관리
#storage.py: S3/Firebase Storage 관련
#database.py: Firestore 데이터베이스 작업
#auth.py: 인증 및 권한 관리
#translation.py: 다국어 번역 기능
#qr_generator.py: QR 코드 생성
#video_handler.py: 비디오 업로드 처리
#certificate.py: 수료증 관련 기능
#scheduler.py: 백그라운드 작업 스케줄링
#utils.py: 공통 유틸리티 함수
#api_routes.py: REST API 엔드포인트
#web_routes.py: 웹 페이지 라우트
#app.py: 메인 애플리케이션