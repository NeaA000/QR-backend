# backend/database.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from firebase_admin import firestore
from datetime import datetime


db = firestore.client()

def get_video_document(group_id):
    """비디오 문서 조회"""
    doc = db.collection('uploads').document(group_id).get()
    return doc.to_dict() if doc.exists else None

def update_video_document(group_id, data):
    """비디오 문서 업데이트"""
    db.collection('uploads').document(group_id).update(data)

def create_video_document(group_id, data):
    """비디오 문서 생성"""
    db.collection('uploads').document(group_id).set(data)

def get_translation(group_id, lang_code):
    """번역 조회"""
    doc = db.collection('uploads').document(group_id) \
           .collection('translations').document(lang_code).get()
    return doc.to_dict() if doc.exists else None

def save_translation(group_id, lang_code, data):
    """번역 저장"""
    db.collection('uploads').document(group_id) \
      .collection('translations').document(lang_code) \
      .set(data)