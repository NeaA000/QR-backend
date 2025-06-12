# backend/storage.py
import boto3
from boto3.s3.transfer import TransferConfig
import firebase_admin
from firebase_admin import credentials, storage as firebase_storage
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, REGION_NAME, BUCKET_NAME, FIREBASE_CREDS

# S3 클라이언트 설정
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=f'https://s3.{REGION_NAME}.wasabisys.com'
)

s3_config = TransferConfig(
    multipart_threshold=1024 * 1024 * 25,
    multipart_chunksize=1024 * 1024 * 50,
    max_concurrency=5,
    use_threads=True
)

# Firebase Storage 초기화
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_CREDS)
    firebase_admin.initialize_app(cred, {
        'storageBucket': f"{FIREBASE_CREDS['project_id']}.appspot.com"
    })

firebase_bucket = firebase_storage.bucket()

def generate_presigned_url(key, expires_in=86400):
    """S3 객체에 대해 presigned URL 생성"""
    return s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=expires_in
    )

def upload_to_s3(file_path, key):
    """파일을 S3에 업로드"""
    s3.upload_file(str(file_path), BUCKET_NAME, key, Config=s3_config)

def upload_to_firebase(file_path, blob_name):
    """파일을 Firebase Storage에 업로드"""
    blob = firebase_bucket.blob(blob_name)
    blob.upload_from_filename(str(file_path))
    return blob.public_url