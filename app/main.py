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

import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.router import api_router


# ─────────────────────────────────────────
# ELK 스택 연동 — JSON 라인 로그 포맷터
# ─────────────────────────────────────────
# Filebeat(VM2) 가 docker json-file 로그를 tail 하여 Logstash(VM3:5044) 로 보낼 때
# Logstash 파이프라인은 fields.log_type == "recommend_json" 분기에서 JSON 필드를
# 그대로 flatten 한다. level/logger/message/timestamp 외에 extra dict 도 top-level
# 필드로 펼쳐 ES 에 색인되도록 한다.
class JsonLineFormatter(logging.Formatter):
    """uvicorn/SQLAlchemy/app 로그를 ELK 친화적 JSON 한 줄로 출력."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # extra 로 전달된 dict(record.__dict__ 기본 키 제외) 를 병합
        reserved = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
            "relativeCreated", "thread", "threadName", "processName", "process", "taskName", "message",
        }
        for key, value in record.__dict__.items():
            if key not in reserved and not key.startswith("_"):
                try:
                    json.dumps(value)  # 직렬화 가능 여부 확인
                    payload[key] = value
                except (TypeError, ValueError):
                    payload[key] = repr(value)
        return json.dumps(payload, ensure_ascii=False)
from app.background.like_flush import register_like_flush_job
from app.v2.api.router import api_v2_router
from app.config import get_settings
from app.core.database import close_db, init_db
from app.core.redis import close_redis, init_redis
from app.core.scheduler import shutdown_scheduler, start_scheduler
from app.search_elasticsearch import ElasticsearchSearchClient
from app.v2.core.database import close_pool, init_pool

# ─────────────────────────────────────────
# 로깅 설정 — JSON (운영) 또는 사람 친화적 텍스트 (개발)
# ─────────────────────────────────────────
# LOG_FORMAT 환경변수로 포맷 선택 — 기본 json. docker-compose/systemd 에서 지정.
import os  # noqa: E402

_log_format = os.getenv("LOG_FORMAT", "json").lower()
_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)

_handler = logging.StreamHandler(sys.stdout)
if _log_format == "json":
    _handler.setFormatter(JsonLineFormatter())
else:
    _handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

_root = logging.getLogger()
_root.handlers.clear()
_root.addHandler(_handler)
_root.setLevel(_log_level)

# 시끄러운 3rd-party 로거 수준 낮춤
for _noisy in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# SQL 쿼리 로깅 (SQL_ECHO=true 일 때만)
# v1: SQLAlchemy 엔진은 `sqlalchemy.engine` 로거로 출력하며,
#     create_async_engine(echo=settings.SQL_ECHO) 설정과 연동된다.
#     echo=True 기본 수준은 INFO 이지만, 파라미터 바인딩까지 보려면 DEBUG 필요.
# v2: `monglepick.recommend.sql` 로거로 LoggingDictCursor 가 쿼리를 DEBUG 로 출력.
#     => 루트 레벨은 INFO 그대로 두고, 두 로거만 DEBUG 로 승격한다.
# ─────────────────────────────────────────
_settings_for_logging = get_settings()
if _settings_for_logging.SQL_ECHO:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)
    logging.getLogger("monglepick.recommend.sql").setLevel(logging.DEBUG)
    logger.info("[SQL_ECHO] v1 SQLAlchemy + v2 aiomysql 쿼리 로깅 활성화")


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

    # MySQL 초기화 — v1(SQLAlchemy) + v2(aiomysql Raw SQL) 동시 초기화
    try:
        await init_db()
        logger.info("[OK] MySQL 비동기 엔진 초기화 완료 (v1 SQLAlchemy)")
    except Exception as e:
        logger.error(f"[FAIL] MySQL v1 초기화 실패: {e}")

    try:
        await init_pool()
        logger.info("[OK] MySQL 커넥션 풀 초기화 완료 (v2 aiomysql Raw SQL)")
    except Exception as e:
        logger.error(f"[FAIL] MySQL v2 초기화 실패: {e}")

    # Redis 초기화
    try:
        await init_redis()
        logger.info("[OK] Redis 커넥션 풀 초기화 완료")
    except Exception as e:
        logger.warning(f"[WARN] Redis 초기화 실패 (DB 폴백 사용): {e}")

    # 백그라운드 스케줄러 시작 + 좋아요 write-behind flush 잡 등록
    # 2026-04-07 신규: Backend 좋아요 도메인 이관에 따른 주기적 DB 반영 필요
    try:
        start_scheduler()
        register_like_flush_job()
        logger.info("[OK] 백그라운드 스케줄러 시작 및 like-flush 잡 등록 완료")
    except Exception as e:
        logger.error(f"[FAIL] 스케줄러 초기화 실패 (write-behind flush 비활성): {e}")

    settings = get_settings()
    logger.info(f"서버: {settings.SERVER_HOST}:{settings.SERVER_PORT}")
    logger.info(f"MySQL: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    logger.info(f"Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
    logger.info(
        "Search ES: enabled=%s url=%s index=%s",
        settings.SEARCH_ES_ENABLED,
        settings.ELASTICSEARCH_URL,
        settings.ELASTICSEARCH_INDEX,
    )
    logger.info("=" * 50)

    yield  # 앱 실행 중

    # ── 종료 ──
    logger.info("몽글픽 추천 서비스 종료 중...")
    # 스케줄러 먼저 종료 (진행 중 flush 완료 대기 → Redis/DB 정리 전에)
    try:
        await shutdown_scheduler()
    except Exception as e:
        logger.warning(f"[WARN] 스케줄러 종료 실패: {e}")
    await ElasticsearchSearchClient.close_shared_client()
    await close_redis()
    await close_pool()   # v2 aiomysql 커넥션 풀 종료
    await close_db()     # v1 SQLAlchemy 엔진 종료
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
# v1: /api/v1/* (SQLAlchemy ORM)
# v2: /api/v2/* (Raw SQL, aiomysql)
# ─────────────────────────────────────────
app.include_router(api_router)
app.include_router(api_v2_router)

# ─────────────────────────────────────────
# Prometheus 메트릭 엔드포인트 (/metrics)
# ─────────────────────────────────────────
# VM3 Prometheus가 http://10.20.0.11:8001/metrics 를 15초 간격으로 스크레이프한다.
# 자동 노출 메트릭:
#   - http_requests_total{handler,method,status}: 요청 수
#   - http_request_duration_seconds_bucket: 응답 시간 히스토그램 (p50/p95/p99)
#   - http_requests_inprogress: 처리 중 요청 수
# Like write-behind flush 관련 커스텀 메트릭은 background/like_flush.py 에서 추가 예정.
Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/health", "/metrics", "/docs", "/redoc", "/openapi.json"],
    inprogress_name="http_requests_inprogress",
    inprogress_labels=True,
).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False, tags=["시스템"])


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
