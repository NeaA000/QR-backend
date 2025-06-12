# backend/certificate.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db
from firebase_admin import firestore
import pandas as pd
import io
from datetime import datetime
from storage import firebase_bucket
import logging

logger = logging.getLogger(__name__)

def create_certificate(user_uid, cert_id, lecture_title, pdf_url):
    """수료증 생성"""
    cert_ref = db.collection('users').document(user_uid) \
                 .collection('completedCertificates').document(cert_id)
    cert_ref.set({
        'lectureTitle': lecture_title,
        'issuedAt': firestore.SERVER_TIMESTAMP,
        'pdfUrl': pdf_url,
        'excelUpdated': False,
        'readyForExcel': True
    }, merge=True)

def add_to_master_excel(user_uid, cert_id):
    """마스터 엑셀에 수료증 추가"""
    try:
        # 수료증 정보 조회
        cert_ref = db.collection('users').document(user_uid) \
                     .collection('completedCertificates').document(cert_id)
        cert_doc = cert_ref.get()
        
        if not cert_doc.exists:
            raise ValueError('수료증이 존재하지 않습니다.')
        
        cert_info = cert_doc.to_dict()
        pdf_url = cert_info.get('pdfUrl', '')
        if not pdf_url:
            raise ValueError('PDF URL이 없습니다.')
        
        lecture_title = cert_info.get('lectureTitle', cert_id)
        issued_at = cert_info.get('issuedAt')
        
        # 타임스탬프 변환
        if hasattr(issued_at, 'to_datetime'):
            issued_dt = issued_at.to_datetime()
        else:
            issued_dt = datetime.utcnow()
        
        # 마스터 엑셀 다운로드/생성
        master_blob_name = 'master_certificates.xlsx'
        master_blob = firebase_bucket.blob(master_blob_name)
        
        try:
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer = io.BytesIO(existing_bytes)
            df_master = pd.read_excel(excel_buffer, engine='openpyxl')
        except Exception:
            df_master = pd.DataFrame(columns=[
                '업데이트 날짜', '사용자 UID', '전화번호', '이메일',
                '사용자 이름', '강의 제목', '발급 일시', 'PDF URL'
            ])
        
        # 사용자 정보 조회
        user_ref = db.collection("users").document(user_uid)
        user_snapshot = user_ref.get()
        if user_snapshot.exists:
            user_data = user_snapshot.to_dict()
            user_name = user_data.get("name", "")
            user_phone = user_data.get("phone", "")
            user_email = user_data.get("email", "")
        else:
            user_name = user_phone = user_email = ""
        
        # 새 행 추가
        updated_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        issued_str = issued_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        new_row = pd.DataFrame([{
            '업데이트 날짜': updated_date,
            '사용자 UID': user_uid,
            '전화번호': user_phone,
            '이메일': user_email,
            '사용자 이름': user_name,
            '강의 제목': lecture_title,
            '발급 일시': issued_str,
            'PDF URL': pdf_url
        }])
        df_master = pd.concat([df_master, new_row], ignore_index=True)
        
        # 엑셀 저장
        out_buffer = io.BytesIO()
        with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
            df_master.to_excel(writer, index=False, sheet_name="Certificates")
        out_buffer.seek(0)
        
        # Firebase Storage에 업로드
        master_blob.upload_from_file(
            out_buffer,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # 플래그 업데이트
        cert_ref.update({
            "excelUpdated": True,
            "readyForExcel": False
        })
        
        logger.info(f"✅ 마스터 엑셀에 추가 완료: {user_uid}/{cert_id}")
        
    except Exception as e:
        logger.error(f"마스터 엑셀 추가 실패: {e}")
        raise