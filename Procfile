web: pip install -r requirements.txt && gunicorn app:app
worker: pip install -r requirements-worker.txt && python worker/firestore_poller.py
