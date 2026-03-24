# =========================================
# 몽글픽 추천 서비스 Docker 이미지
# =========================================
# 멀티스테이지 빌드: 의존성 설치 → 런타임 이미지
# Python 3.12 slim 기반, 비루트 사용자로 실행

# --- 1단계: 의존성 설치 ---
FROM python:3.12-slim AS builder

WORKDIR /app

# pip 업그레이드 및 의존성 설치 (캐시 레이어 활용)
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- 2단계: 런타임 이미지 ---
FROM python:3.12-slim

WORKDIR /app

# 빌드 단계에서 설치한 패키지 복사
COPY --from=builder /install /usr/local

# 애플리케이션 코드 복사
COPY . .

# 비루트 사용자 생성 (보안)
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# 헬스체크용 포트 노출
EXPOSE 8001

# uvicorn으로 FastAPI 앱 실행
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
