# Procfile (프로젝트 루트에 위치)
web:    pip install -r requirements.txt && gunicorn app:app --bind 0.0.0.0:$PORT
worker: pip install -r requirements-worker.txt && python worker/firestore_poller.py
