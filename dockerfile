
#FROM python:3.11-slim

#RUN apt-get update && apt-get install -y \
#    fonts-noto-cjk \
#    fonts-noto-cjk-extra \
#    fontconfig \
#    && fc-cache -fv \
#    && rm -rf /var/lib/apt/lists/*

#WORKDIR /app
#COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt
#COPY . .
#RUN mkdir -p fonts static

#EXPOSE 8080
#CMD ["python", "app.py"]
FROM python:3.11-slim

# 1) OS 레벨 의존성 한 번에 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      # 이미지·비디오 처리
      ffmpeg \
      libjpeg-dev zlib1g-dev \
      # 암호화·SSL
      libssl-dev libffi-dev \
      python3-dev build-essential \
      # 기존에 쓰시던 폰트
      fonts-noto-cjk fonts-noto-cjk-extra fontconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) 의존성 설치
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# 3) 소스 복사
COPY . .

# 4) 정적 폴더 생성 (필요 시)
RUN mkdir -p fonts static

EXPOSE 8080
CMD ["python", "app.py"]
