# worker/firestore_poller.py - 수정된 버전

import os
import time
import io
import warnings
import pandas as pd
from datetime import datetime, timezone
import signal
import sys
import logging
from typing import List, Tuple, Dict, Any

# Firebase Admin SDK 사용 (certificate_worker와 동일하게)
import firebase_admin
from firebase_admin import credentials, firestore, storage

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정 개선
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('worker.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('FirestorePoller')

# ─────────────────────────────────────────────────────────────────────────────
# 환경변수 및 설정
# ─────────────────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
MASTER_FILENAME = "master_certificates.xlsx"

# ─────────────────────────────────────────────────────────────────────────────
# Firebase 초기화 (certificate_worker와 동일한 방식)
# ─────────────────────────────────────────────────────────────────────────────
def initialize_firebase():
    """Firebase Admin SDK 초기화"""
    try:
        if not firebase_admin._apps:
            # Railway 환경변수에서 자격증명 생성
            firebase_creds = {
                "type": os.environ.get("type", "service_account"),
                "project_id": os.environ["project_id"],
                "private_key_id": os.environ.get("private_key_id", ""),
                "private_key": os.environ["private_key"].replace('\\n', '\n'),
                "client_email": os.environ["client_email"],
                "client_id": os.environ.get("client_id", ""),
                "auth_uri": os.environ.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
                "token_uri": os.environ.get("token_uri", "https://oauth2.googleapis.com/token"),
                "auth_provider_x509_cert_url": os.environ.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
                "client_x509_cert_url": os.environ.get("client_x509_cert_url", "")
            }
            
            cred = credentials.Certificate(firebase_creds)
            firebase_admin.initialize_app(cred, {
                'storageBucket': f"{os.environ['project_id']}.appspot.com"
            })
            
        db = firestore.client()
        bucket = storage.bucket()
        
        logger.info(f"✅ Firebase 초기화 완료 - Project: {os.environ['project_id']}")
        return db, bucket
        
    except Exception as e:
        logger.error(f"❌ Firebase 초기화 실패: {e}")
        raise

# Firebase 클라이언트 초기화
db, bucket = initialize_firebase()

# ─────────────────────────────────────────────────────────────────────────────
# 전역 변수: 정상 종료 시그널 처리
# ─────────────────────────────────────────────────────────────────────────────
shutdown_flag = False

def signal_handler(signum, frame):
    """SIGINT/SIGTERM 시그널 핸들러"""
    global shutdown_flag
    logger.info(f"🛑 종료 시그널 받음 ({signum}). 안전하게 종료합니다...")
    shutdown_flag = True

# 시그널 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ─────────────────────────────────────────────────────────────────────────────
# 헬스체크
# ─────────────────────────────────────────────────────────────────────────────
def update_health_status():
    """헬스체크 파일 업데이트"""
    try:
        with open('/tmp/worker_healthy', 'w') as f:
            f.write(f"healthy at {datetime.utcnow().isoformat()}")
    except Exception as e:
        logger.warning(f"헬스 파일 업데이트 실패: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 수료증 조회 및 처리 함수들
# ─────────────────────────────────────────────────────────────────────────────
def fetch_unprocessed_certs(limit=100) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    collection_group을 사용해서 users/{uid}/completedCertificates 아래의 모든 문서를 조회.
    excelUpdated가 False이고 readyForExcel이 True인 문서만 결과 리스트에 추가.
    반환 값: [(user_uid, cert_id, cert_data_dict), ...]
    """
    results = []
    try:
        # readyForExcel=True이고 excelUpdated=False인 문서만 조회 (더 효율적)
        query = db.collection_group("completedCertificates") \
                  .where("readyForExcel", "==", True) \
                  .where("excelUpdated", "==", False) \
                  .limit(limit)
        
        all_certs = list(query.stream())
        logger.debug(f"🔍 쿼리 결과: {len(all_certs)}개 문서")
        
    except Exception as e:
        logger.error(f"❌ collection_group 쿼리 실패: {e}")
        return results

    for cert_doc in all_certs:
        try:
            data = cert_doc.to_dict()
            
            # 추가 검증: PDF URL이 있는지 확인
            if not data.get("pdfUrl"):
                logger.warning(f"⚠️ 문서 {cert_doc.id}에 PDF URL이 없습니다. 건너뜁니다.")
                # 에러 상태로 마킹
                _mark_as_error(cert_doc.reference, "PDF URL이 없음")
                continue

            # 문서 경로 예시: "users/{userId}/completedCertificates/{certId}"
            path_parts = cert_doc.reference.path.split("/")
            if len(path_parts) < 4:
                logger.warning(f"⚠️ 잘못된 문서 경로: {cert_doc.reference.path}")
                continue
                
            user_uid = path_parts[1]
            cert_id  = path_parts[3]

            results.append((user_uid, cert_id, data))
            
        except Exception as e:
            logger.error(f"❌ 문서 파싱 실패 {cert_doc.id}: {e}")

    logger.info(f"📋 {len(results)}개의 처리 대기 수료증 발견")
    return results

def get_user_info(user_uid: str) -> Dict[str, str]:
    """사용자 정보 조회"""
    try:
        user_ref = db.collection("users").document(user_uid)
        user_snapshot = user_ref.get()
        
        if user_snapshot.exists:
            user_data = user_snapshot.to_dict()
            return {
                'name': user_data.get("name", ""),
                'phone': user_data.get("phone", ""),
                'email': user_data.get("email", "")
            }
        else:
            logger.warning(f"⚠️ 사용자 문서 {user_uid}가 존재하지 않습니다.")
            return {'name': "", 'phone': "", 'email': ""}
            
    except Exception as e:
        logger.error(f"❌ 사용자 정보 조회 실패 ({user_uid}): {e}")
        return {'name': "", 'phone': "", 'email': ""}

def get_or_create_master_excel():
    """마스터 엑셀 파일 가져오기 또는 생성"""
    try:
        master_blob = bucket.blob(MASTER_FILENAME)
        
        # 기존 파일 다운로드 시도
        try:
            existing_bytes = master_blob.download_as_bytes()
            excel_buffer = io.BytesIO(existing_bytes)
            df = pd.read_excel(excel_buffer, engine="openpyxl")
            logger.debug(f"📥 기존 마스터 엑셀 로드 완료 (행 수: {len(df)})")
            
            # 기존 DataFrame에서 불필요한 열 삭제 (certificate_worker와 동일하게)
            columns_to_remove = ['User UID', 'Lecture Title', 'Issued At']
            for col in columns_to_remove:
                if col in df.columns:
                    df = df.drop(columns=[col])
                    logger.debug(f"🗑️ 불필요한 컬럼 제거: {col}")
            
        except Exception as e:
            # 새 DataFrame 생성
            logger.info(f"📄 새 마스터 엑셀 파일 생성: {e}")
            df = pd.DataFrame(columns=[
                '업데이트 날짜',
                '사용자 UID',
                '전화번호',
                '이메일',
                '사용자 이름',
                '강의 제목',
                '발급 일시',
                'PDF URL',
                'Cert ID'
            ])

        # 'Cert ID' 컬럼 확인 및 생성
        if 'Cert ID' not in df.columns:
            df['Cert ID'] = ""
            logger.debug("📋 'Cert ID' 컬럼 추가")
            
        return df
        
    except Exception as e:
        logger.error(f"❌ 마스터 엑셀 처리 실패: {e}")
        raise

def save_master_excel(df):
    """마스터 엑셀 파일 저장"""
    try:
        # DataFrame을 엑셀로 변환
        out_buffer = io.BytesIO()
        with pd.ExcelWriter(out_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Certificates")
        out_buffer.seek(0)
        
        # Firebase Storage에 업로드
        master_blob = bucket.blob(MASTER_FILENAME)
        master_blob.upload_from_file(
            out_buffer,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        logger.info("✅ 마스터 엑셀 저장 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ 마스터 엑셀 저장 실패: {e}")
        return False

def update_excel_for_cert(user_uid: str, cert_id: str, cert_info: Dict[str, Any], df) -> Tuple[bool, any]:
    """
    수료증 정보를 마스터 엑셀에 추가
    
    Returns:
        Tuple[bool, DataFrame]: (성공여부, 업데이트된 DataFrame)
    """
    try:
        # --- 1) 사용자 프로필 정보 조회 ---
        user_info = get_user_info(user_uid)

        # --- 2) 수료증 정보 추출 ---
        lecture_title = cert_info.get("lectureTitle", cert_id)
        issued_at_ts = cert_info.get("issuedAt")
        pdf_url = cert_info.get("pdfUrl", "")

        # PDF URL 재검증
        if not pdf_url:
            logger.error(f"❌ cert {cert_id}에 PDF URL이 없습니다")
            return False, df

        # Firestore Timestamp → 문자열
        if hasattr(issued_at_ts, "to_datetime"):
            issued_str = issued_at_ts.to_datetime().strftime("%Y-%m-%d %H:%M:%S")
        else:
            issued_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # --- 3) 중복 체크 (안전한 방식) ---
        if not df.empty and 'Cert ID' in df.columns:
            existing_cert_ids = df['Cert ID'].astype(str).values
            if cert_id in existing_cert_ids:
                logger.warning(f"⚠️ cert_id={cert_id}가 이미 엑셀에 존재합니다. 처리 완료로 마킹합니다.")
                _mark_as_processed(user_uid, cert_id)
                return True, df

        # --- 4) 새 행 생성 및 추가 ---
        updated_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        new_row = pd.DataFrame([{
            '업데이트 날짜': updated_date,
            '사용자 UID': user_uid,
            '전화번호': user_info['phone'],
            '이메일': user_info['email'],
            '사용자 이름': user_info['name'],
            '강의 제목': lecture_title,
            '발급 일시': issued_str,
            'PDF URL': pdf_url,
            'Cert ID': cert_id
        }])

        df = pd.concat([df, new_row], ignore_index=True)
        logger.debug(f"➕ 새 행 추가: {user_uid}/{cert_id}. 총 행 수: {len(df)}")

        # --- 5) Firestore 플래그 업데이트 ---
        if _mark_as_processed(user_uid, cert_id):
            logger.info(f"✅ 수료증 처리 완료: {user_uid}/{cert_id} - {lecture_title}")
            return True, df
        else:
            return False, df
        
    except Exception as e:
        logger.error(f"❌ 수료증 처리 중 예상치 못한 오류 ({user_uid}/{cert_id}): {e}")
        _mark_as_error_with_details(user_uid, cert_id, str(e))
        return False, df

def _mark_as_processed(user_uid: str, cert_id: str) -> bool:
    """Firestore 문서에 처리 완료 플래그 설정"""
    try:
        cert_ref = db.collection("users").document(user_uid) \
                     .collection("completedCertificates").document(cert_id)
        cert_ref.update({
            "excelUpdated": True,
            "readyForExcel": False,
            "processedAt": firestore.SERVER_TIMESTAMP,
            "processedBy": "firestore_poller"
        })
        logger.debug(f"✅ {user_uid}/{cert_id} 처리 완료로 마킹")
        return True
    except Exception as e:
        logger.error(f"❌ 플래그 업데이트 실패 ({user_uid}/{cert_id}): {e}")
        return False

def _mark_as_error(doc_ref, error_message: str):
    """문서에 에러 상태 마킹"""
    try:
        doc_ref.update({
            "excelUpdateError": error_message,
            "errorOccurredAt": firestore.SERVER_TIMESTAMP,
            "readyForExcel": False
        })
    except Exception as e:
        logger.error(f"에러 마킹 실패: {e}")

def _mark_as_error_with_details(user_uid: str, cert_id: str, error_message: str):
    """Firestore 문서에 에러 상태 마킹"""
    try:
        cert_ref = db.collection("users").document(user_uid) \
                     .collection("completedCertificates").document(cert_id)
        cert_ref.update({
            "excelUpdateError": error_message,
            "errorOccurredAt": firestore.SERVER_TIMESTAMP,
            "readyForExcel": False
        })
    except Exception as e:
        logger.error(f"에러 마킹 실패 ({user_uid}/{cert_id}): {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 배치 처리 함수
# ─────────────────────────────────────────────────────────────────────────────
def process_batch():
    """배치 처리 실행"""
    try:
        # 처리할 수료증 조회
        unprocessed = fetch_unprocessed_certs(limit=BATCH_SIZE)
        
        if not unprocessed:
            logger.debug("😴 처리할 수료증이 없습니다")
            return
        
        # 마스터 엑셀 로드
        df = get_or_create_master_excel()
        
        # 처리 통계
        success_count = 0
        error_count = 0
        
        # 각 수료증 처리
        for user_uid, cert_id, cert_info in unprocessed:
            if shutdown_flag:
                logger.info("🛑 종료 플래그 감지, 처리 중단")
                break
                
            success, df = update_excel_for_cert(user_uid, cert_id, cert_info, df)
            
            if success:
                success_count += 1
            else:
                error_count += 1
        
        # 변경사항이 있으면 저장
        if success_count > 0:
            if save_master_excel(df):
                logger.info(f"📊 배치 처리 완료 - 성공: {success_count}, 실패: {error_count}")
            else:
                logger.error("❌ 마스터 엑셀 저장 실패")
        
    except Exception as e:
        logger.error(f"❌ 배치 처리 중 오류: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 헬스체크 및 통계 함수들
# ─────────────────────────────────────────────────────────────────────────────
def get_pending_count() -> int:
    """처리 대기 중인 수료증 개수 조회"""
    try:
        query = db.collection_group("completedCertificates") \
                  .where("readyForExcel", "==", True) \
                  .where("excelUpdated", "==", False)
        return len(list(query.stream()))
    except Exception as e:
        logger.error(f"❌ 대기 수 조회 실패: {e}")
        return -1

def log_statistics():
    """워커 통계 로깅"""
    try:
        pending_count = get_pending_count()
        if pending_count >= 0:
            logger.info(f"📊 대기 중인 수료증: {pending_count}개")
        else:
            logger.warning(f"⚠️ 대기 수를 가져올 수 없습니다")
    except Exception as e:
        logger.error(f"❌ 통계 로깅 오류: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 메인 루프: 주기적으로 폴링 실행
# ─────────────────────────────────────────────────────────────────────────────
def run_poller():
    """메인 폴링 루프"""
    logger.info(f"🚀 Firestore Poller 시작 (간격: {POLL_INTERVAL_SECONDS}초, 배치 크기: {BATCH_SIZE})")
    
    # 시작 시 통계 로깅
    log_statistics()
    
    # 초기 헬스 상태
    update_health_status()
    
    iteration_count = 0
    successful_updates = 0
    failed_updates = 0
    
    while not shutdown_flag:
        try:
            iteration_count += 1
            logger.debug(f"🔄 폴링 반복 #{iteration_count}")
            
            # 헬스체크 업데이트
            update_health_status()
            
            # 배치 처리 실행
            process_batch()
            
            # 10번째 반복마다 통계 로깅
            if iteration_count % 10 == 0:
                logger.info(f"📈 상태 - 처리: {successful_updates}, 실패: {failed_updates}")
                log_statistics()
            
            # 종료 시그널 체크하면서 대기
            for _ in range(POLL_INTERVAL_SECONDS):
                if shutdown_flag:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⌨️ 키보드 인터럽트")
            break
        except Exception as e:
            failed_updates += 1
            logger.error(f"❌ 폴링 루프 오류: {e}")
            time.sleep(min(POLL_INTERVAL_SECONDS, 30))  # 에러 시 최대 30초만 대기

    logger.info(f"🏁 Poller 종료. 최종 통계 - 성공: {successful_updates}, 실패: {failed_updates}")

if __name__ == "__main__":
    try:
        run_poller()
    except Exception as e:
        logger.error(f"❌ 워커 시작 실패: {e}")
        sys.exit(1)
    finally:
        logger.info("👋 워커 종료 완료")