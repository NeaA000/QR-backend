# backend/api_routes.py
from flask import Blueprint, request, jsonify, abort
from auth import admin_required
from database import db
from storage import generate_presigned_url
from scheduler import is_presigned_url_expired, refresh_expiring_urls, scheduler
from utils import get_video_with_translation
from config import SUPPORTED_LANGUAGES
from certificate import create_certificate, add_to_master_excel
from datetime import datetime
import threading
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__)

@api_bp.route('/admin/login', methods=['POST'])
def api_admin_login():
    """관리자 로그인 API"""
    from auth import create_jwt_for_admin, ADMIN_EMAIL, ADMIN_PASSWORD
    
    data = request.get_json() or {}
    email = data.get('email', '').strip()
    password = data.get('password', '')
    
    if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
        token = create_jwt_for_admin()
        return jsonify({'token': token}), 200
    else:
        return jsonify({'error': '관리자 인증 실패'}), 401

@api_bp.route('/admin/refresh-urls', methods=['POST'])
@admin_required
def manual_refresh_urls():
    """수동 URL 갱신"""
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

@api_bp.route('/admin/scheduler-status', methods=['GET'])
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

@api_bp.route('/videos/<group_id>', methods=['GET'])
def get_video_detail(group_id):
    """비디오 상세 정보 API"""
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

# 수료증 관련 API
@api_bp.route('/create_certificate', methods=['POST'])
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

@api_bp.route('/add_certificate_to_master', methods=['POST'])
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