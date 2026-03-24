"""
몽글픽 추천 서비스 메인 애플리케이션

FastAPI 앱 인스턴스를 생성하고 다음을 설정합니다:
- lifespan: 앱 시작/종료 시 DB 및 Redis 초기화/정리
- CORS 미들웨어: 프론트엔드 도메인 허용
- 라우터: /api/v1 하위에 검색 + 온보딩 엔드포인트 등록
- 헬스체크: GET /health

실행 방법:
    uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config import get_settings
from app.core.database import close_db, init_db
from app.core.redis import close_redis, init_redis

# ─────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 애플리케이션 수명 주기 관리

    앱 시작 시:
    1. MySQL 비동기 엔진 초기화 + 이 서비스 소유 테이블 생성
    2. Redis 커넥션 풀 초기화 + ping 확인

    앱 종료 시:
    1. Redis 커넥션 풀 정리
    2. MySQL 엔진 커넥션 풀 정리
    """
    # ── 시작 ──
    logger.info("=" * 50)
    logger.info("몽글픽 추천 서비스 시작 중...")
    logger.info("=" * 50)

    # MySQL 초기화 (search_history, trending_keywords 등 테이블 생성)
    try:
        await init_db()
        logger.info("[OK] MySQL 비동기 엔진 초기화 완료")
    except Exception as e:
        logger.error(f"[FAIL] MySQL 초기화 실패: {e}")
        # DB 없이도 일부 기능(헬스체크 등)은 동작하도록 에러 전파하지 않음

    # Redis 초기화
    try:
        await init_redis()
        logger.info("[OK] Redis 커넥션 풀 초기화 완료")
    except Exception as e:
        logger.warning(f"[WARN] Redis 초기화 실패 (DB 폴백 사용): {e}")

    settings = get_settings()
    logger.info(f"서버: {settings.SERVER_HOST}:{settings.SERVER_PORT}")
    logger.info(f"MySQL: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    logger.info(f"Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
    logger.info("=" * 50)

    yield  # 앱 실행 중

    # ── 종료 ──
    logger.info("몽글픽 추천 서비스 종료 중...")
    await close_redis()
    await close_db()
    logger.info("리소스 정리 완료")


# ─────────────────────────────────────────
# FastAPI 앱 생성
# ─────────────────────────────────────────
app = FastAPI(
    title="몽글픽 추천 서비스",
    description=(
        "영화 검색(REQ_031~034) 및 회원 개인화 초기 설정(REQ_016~019) API.\n\n"
        "Spring Boot 백엔드(monglepick-backend)와 MySQL DB를 공유하며,\n"
        "JWT 토큰도 동일한 시크릿으로 검증합니다."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────
# CORS 미들웨어 설정
# Spring Boot 백엔드의 CORS 설정과 동일한 오리진 허용
# ─────────────────────────────────────────
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,  # 쿠키/Authorization 헤더 허용
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,  # preflight 캐시 1시간
)

# ─────────────────────────────────────────
# API 라우터 등록
# ─────────────────────────────────────────
app.include_router(api_router)


# ─────────────────────────────────────────
# 헬스체크 엔드포인트
# ─────────────────────────────────────────
@app.get(
    "/health",
    tags=["시스템"],
    summary="헬스체크",
    description="서비스 상태를 확인합니다.",
)
async def health_check():
    """
    헬스체크 엔드포인트

    로드밸런서/컨테이너 오케스트레이션에서 사용합니다.
    DB/Redis 연결 상태와 무관하게 앱 자체가 살아있으면 200을 반환합니다.
    """
    return {
        "status": "healthy",
        "service": "monglepick-recommend",
        "version": "0.1.0",
    }
