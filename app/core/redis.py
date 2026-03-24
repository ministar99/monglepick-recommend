"""
Redis 비동기 커넥션 풀 관리 모듈

인기 검색어(Sorted Set), 자동완성 캐시, 온보딩 임시 데이터 등에 사용합니다.
monglepick-agent는 Redis DB 0번을 사용하므로, 이 서비스는 DB 1번을 사용합니다.

주요 Redis 키 패턴:
- trending:keywords         → Sorted Set (인기 검색어, score=검색 횟수)
- autocomplete:{prefix}     → String/JSON (자동완성 캐시, TTL 5분)
- recent:{user_id}          → List (최근 검색어, 최대 20개)
- worldcup:{user_id}        → Hash (월드컵 진행 상태)
"""

import redis.asyncio as aioredis

from app.config import get_settings

# ─────────────────────────────────────────
# Redis 비동기 커넥션 풀 (모듈 레벨 싱글턴)
# ─────────────────────────────────────────
_redis_pool: aioredis.Redis | None = None


async def init_redis() -> aioredis.Redis:
    """
    Redis 커넥션 풀을 초기화하고 반환합니다.

    애플리케이션 시작 시(lifespan) 한 번 호출됩니다.
    max_connections=50으로 동시 접속 제한합니다.

    Returns:
        aioredis.Redis: Redis 비동기 클라이언트
    """
    global _redis_pool
    settings = get_settings()
    _redis_pool = aioredis.from_url(
        settings.redis_url,
        max_connections=50,
        decode_responses=True,  # 바이트 대신 문자열로 디코딩
        encoding="utf-8",
    )
    # 연결 확인 (ping 실패 시 예외 발생)
    await _redis_pool.ping()
    return _redis_pool


async def get_redis() -> aioredis.Redis:
    """
    현재 Redis 커넥션 풀을 반환합니다.

    FastAPI Depends()에서 사용합니다.
    init_redis()가 호출되지 않은 상태에서 접근하면 예외를 발생시킵니다.

    Returns:
        aioredis.Redis: Redis 비동기 클라이언트

    Raises:
        RuntimeError: Redis가 초기화되지 않은 경우
    """
    if _redis_pool is None:
        raise RuntimeError("Redis가 초기화되지 않았습니다. init_redis()를 먼저 호출하세요.")
    return _redis_pool


async def close_redis() -> None:
    """Redis 커넥션 풀을 정리합니다. 앱 종료 시 호출됩니다."""
    global _redis_pool
    if _redis_pool is not None:
        await _redis_pool.close()
        _redis_pool = None
