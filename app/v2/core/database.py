"""
aiomysql 커넥션 풀 직접 관리 (Raw SQL 전용)

SQLAlchemy를 사용하지 않고 aiomysql.create_pool()로
커넥션 풀을 직접 생성하고 관리합니다.

커넥션 풀 설정은 v1(SQLAlchemy)과 동일하게 유지합니다:
- pool_size=20: Spring Boot HikariCP maximum-pool-size와 동일
- pool_recycle_time=1800: 30분마다 커넥션 재생성 (MySQL wait_timeout 대응)

사용법 (FastAPI Depends):
    async def get_conn():
        async with pool.acquire() as conn:
            yield conn
"""

import logging
from typing import Optional

import aiomysql

from app.config import get_settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 모듈 레벨 커넥션 풀 (싱글턴)
# init_pool()에서 생성, close_pool()에서 정리
# ─────────────────────────────────────────
_pool: Optional[aiomysql.Pool] = None


async def init_pool() -> None:
    """
    aiomysql 커넥션 풀을 초기화합니다.

    FastAPI lifespan 시작 시 호출합니다.
    이미 초기화된 경우 중복 생성하지 않습니다.
    """
    global _pool
    if _pool is not None:
        return

    settings = get_settings()
    _pool = await aiomysql.create_pool(
        host=settings.DB_HOST,
        port=int(settings.DB_PORT),
        user=settings.DB_USERNAME,
        password=settings.DB_PASSWORD,
        db=settings.DB_NAME,
        charset="utf8mb4",
        # ── 커넥션 풀 설정 (v1 SQLAlchemy와 동일) ──
        minsize=5,           # 최소 유휴 커넥션 수
        maxsize=20,          # 최대 커넥션 수 (HikariCP parity)
        pool_recycle=1800,   # 30분마다 커넥션 재생성 (MySQL wait_timeout 대응)
        autocommit=False,    # 수동 커밋 (트랜잭션 제어)
        # DictCursor를 기본으로 사용하여 결과를 딕셔너리로 반환
        cursorclass=aiomysql.DictCursor,
    )
    logger.info("[v2] aiomysql 커넥션 풀 초기화 완료 (maxsize=20)")


async def get_pool() -> aiomysql.Pool:
    """
    초기화된 커넥션 풀을 반환합니다.

    Returns:
        aiomysql.Pool: 커넥션 풀

    Raises:
        RuntimeError: init_pool()이 호출되지 않은 경우
    """
    if _pool is None:
        raise RuntimeError(
            "[v2] aiomysql 커넥션 풀이 초기화되지 않았습니다. "
            "init_pool()을 먼저 호출하세요."
        )
    return _pool


async def close_pool() -> None:
    """
    커넥션 풀을 종료하고 모든 커넥션을 반환합니다.

    FastAPI lifespan 종료 시 호출합니다.
    """
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("[v2] aiomysql 커넥션 풀 종료 완료")
