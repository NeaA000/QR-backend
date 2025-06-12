# backend/scheduler.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from database import db
from storage import generate_presigned_url
import logging
import atexit

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(
    timezone='UTC',
    job_defaults={
        'coalesce': True,
        'max_instances': 1
    }
)

def is_presigned_url_expired(url, safety_margin_minutes=60):
    """Presigned URL 만료 확인"""
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
    """만료 임박한 URL 갱신"""
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
    """스케줄러 시작"""
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