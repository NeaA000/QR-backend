# ─────────── Procfile ───────────
# 하나의 서비스로 통합해서 실행하고 싶다면 아래처럼 작성하세요.

web: pip install -r requirements.txt && \
     pip install -r worker/requirements-worker.txt && \
     python worker/firestore_poller.py & \
     python app.py
