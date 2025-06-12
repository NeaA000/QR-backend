# backend/video_handler.py
import os
import uuid
import re
import tempfile
from pathlib import Path
from datetime import datetime
from moviepy.editor import VideoFileClip
import logging
from config import ALLOWED_VIDEO_EXTENSIONS, ALLOWED_IMAGE_EXTENSIONS, APP_BASE_URL, SUPPORTED_LANGUAGES
from storage import upload_to_s3, upload_to_firebase, generate_presigned_url, s3, BUCKET_NAME
from database import create_video_document, save_translation
from translation import create_multilingual_metadata
from qr_generator import create_qr_with_logo

logger = logging.getLogger(__name__)

def get_video_duration(file_path):
    """비디오 길이 가져오기 (형식 무관)"""
    try:
        with VideoFileClip(str(file_path)) as clip:
            duration_sec = int(clip.duration)
            minutes = duration_sec // 60
            seconds = duration_sec % 60
            return f"{minutes}:{seconds:02d}", duration_sec
    except Exception as e:
        logger.warning(f"비디오 길이 가져오기 실패: {e}")
        return "0:00", 0

def is_allowed_file(filename, allowed_extensions):
    """파일 확장자 확인"""
    return Path(filename).suffix.lower() in allowed_extensions

def process_video_upload(file, group_name, main_cat, sub_cat, leaf_cat, level, tag, thumbnail_file=None):
    """비디오 업로드 처리"""
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