# backend/utils.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from datetime import datetime, date, timedelta
from database import db, get_translation
from config import SUPPORTED_LANGUAGES
import logging

logger = logging.getLogger(__name__)

def get_video_with_translation(group_id, lang_code='ko'):
    """특정 언어로 비디오 정보 조회"""
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

def parse_iso_week(week_str):
    """ISO 주차 파싱"""
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