# ─────────────────────────────────────────────────────────────────────────────
# Procfile (프로젝트 루트에 위치)
# ─────────────────────────────────────────────────────────────────────────────

# "web" 프로세스: requirements.txt를 설치한 뒤, Gunicorn으로 Flask 앱을 실행
web: pip install -r requirements.txt && gunicorn backend.app:app --bind 0.0.0.0:$PORT

# "worker" 프로세스: requirements-worker.txt를 설치한 뒤, 폴링 스크립트를 실행
worker: pip install -r requirements-worker.txt && python backend/worker/firestore_poller.py