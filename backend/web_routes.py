# backend/web_routes.py
from flask import Blueprint, render_template, request, redirect, url_for, session, abort
from auth import session_required
from video_handler import process_video_upload
from utils import get_video_with_translation
from scheduler import is_presigned_url_expired
from storage import generate_presigned_url
from database import db
from config import SUPPORTED_LANGUAGES, ADMIN_EMAIL, ADMIN_PASSWORD
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

web_bp = Blueprint('web', __name__)

@web_bp.route('/')
def login_page():
    """로그인 페이지"""
    return render_template('login.html')

@web_bp.route('/login', methods=['POST'])
def login():
    """로그인 처리"""
    pw = request.form.get('password', '')
    email = request.form.get('email', '')
    
    if email == ADMIN_EMAIL and pw == ADMIN_PASSWORD:
        session['logged_in'] = True
        return redirect(url_for('web.upload_form'))
    return render_template('login.html', error="이메일 또는 비밀번호가 올바르지 않습니다.")

@web_bp.route('/upload_form')
@session_required
def upload_form():
    """업로드 폼"""
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

@web_bp.route('/upload', methods=['POST'])
@session_required
def upload_video():
    """비디오 업로드"""
    file = request.files.get('file')
    thumbnail = request.files.get('thumbnail')  # 썸네일 파일
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

@web_bp.route('/watch/<group_id>')
def watch(group_id):
    """비디오 시청 페이지"""
    requested_lang = request.args.get('lang', 'ko')
    
    if requested_lang not in SUPPORTED_LANGUAGES:
        requested_lang = 'ko'
    
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
        from flask import jsonify
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