# =========================================
# 몽글픽 추천 서비스 Docker 이미지
# =========================================
# 멀티스테이지 빌드: uv 의존성 설치 → 런타임 이미지
# Python 3.12 slim 기반, 비루트 사용자로 실행
#
# 빌드: docker build -t monglepick-recommend .
# 실행: docker run -p 8001:8001 monglepick-recommend

# --- 1단계: 의존성 설치 ---
FROM python:3.12-slim AS builder

WORKDIR /app

# uv 설치 (빠른 패키지 매니저) — agent 서비스와 동일 전략
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 의존성 파일 복사 (캐시 레이어 활용)
COPY pyproject.toml uv.lock ./

# 의존성만 설치 (프로젝트 자체는 app/ 레이아웃이라 설치 대상 아님).
# --frozen: uv.lock 을 진실원본으로 강제, 드리프트 방지.
# --no-dev: 운영 런타임엔 dev 의존성 제외.
# --no-install-project: src layout 이 아니므로 프로젝트 자체 빌드/설치는 생략하고
#                       .venv 에 dependency 만 설치한다.
RUN uv sync --frozen --no-dev --no-install-project

# --- 2단계: 런타임 이미지 ---
FROM python:3.12-slim

WORKDIR /app

# curl 설치 (헬스체크용)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# 빌드 단계에서 생성된 가상환경 복사
COPY --from=builder /app/.venv /app/.venv

# 애플리케이션 코드 복사 (app 패키지만 넣으면 `app.main:app` 로딩 가능)
COPY app/ app/

# PATH 에 venv 추가 — 시스템 python 대신 .venv 실행되도록
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# 비루트 사용자 생성 (보안)
RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app
USER appuser

# 헬스체크용 포트 노출
EXPOSE 8001

# uvicorn 으로 FastAPI 앱 실행
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
