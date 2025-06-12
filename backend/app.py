# backend/app.py (메인 애플리케이션)
from flask import Flask
from config import SECRET_KEY, UPLOAD_FOLDER, MAX_CONTENT_LENGTH
from qr_generator import download_korean_font
from scheduler import start_scheduler
from api_routes import api_bp
from web_routes import web_bp
import os
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Flask 앱 생성
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Blueprint 등록
app.register_blueprint(api_bp, url_prefix='/api')
app.register_blueprint(web_bp)

# 헬스체크
@app.route('/health', methods=['GET'])
def health_check():
    """서비스 상태 확인"""
    from database import db
    from storage import s3, BUCKET_NAME
    from datetime import datetime
    
    try:
        db.collection('uploads').limit(1).get()
        firestore_status = 'healthy'
    except Exception:
        firestore_status = 'unhealthy'
    
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        s3_status = 'healthy'
    except Exception:
        s3_status = 'unhealthy'
    
    overall_status = 'healthy' if (firestore_status == 'healthy' and s3_status == 'healthy') else 'unhealthy'
    
    return {
        'status': overall_status,
        'timestamp': datetime.utcnow().isoformat(),
        'services': {
            'firestore': firestore_status,
            's3': s3_status,
            'scheduler': scheduler.running if 'scheduler' in globals() else False
        },
        'version': '3.0.0-modular'
    }, 200 if overall_status == 'healthy' else 503

def initialize_app():
    """앱 초기화"""
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
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        app.run(host="0.0.0.0", port=port, debug=True)