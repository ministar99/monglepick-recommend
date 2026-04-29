"""
v2 FastAPI 의존성 주입 모듈

v1(SQLAlchemy AsyncSession)을 대체하여
aiomysql Connection을 요청 스코프로 주입합니다.

트랜잭션 관리:
- 요청 성공 시 자동 커밋
- 예외 발생 시 자동 롤백

인증(JWT), Redis 의존성은 v1과 동일하게 공유합니다.
"""

from collections.abc import AsyncGenerator
import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.redis import get_redis
from app.core.security import verify_token
from app.v2.core.database import get_pool

logger = logging.getLogger(__name__)

# HTTP Bearer 토큰 추출기 (v1과 동일)
_bearer_scheme = HTTPBearer(auto_error=False)
_bearer_scheme_required = HTTPBearer(auto_error=True)


async def get_conn() -> AsyncGenerator[aiomysql.Connection, None]:
    """
    aiomysql 커넥션을 요청 스코프로 주입합니다.

    풀에서 커넥션을 획득하고, 요청 완료 시 자동으로 커밋/롤백/반환합니다.
    v1의 get_db() (AsyncSession)에 대응합니다.

    Yields:
        aiomysql.Connection: MySQL 비동기 커넥션 (DictCursor 기본)
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            yield conn
            # 정상 완료 시 커밋
            await conn.commit()
        except Exception:
            # 예외 발생 시 롤백
            await conn.rollback()
            raise


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme_required),
) -> str:
    """
    JWT 토큰을 검증하고 사용자 ID를 반환합니다.

    v1과 동일한 로직입니다. Spring Boot 백엔드와 동일한 시크릿으로 검증합니다.

    Args:
        credentials: HTTP Bearer 토큰 (FastAPI가 자동 추출)

    Returns:
        str: 사용자 고유 ID (VARCHAR(50))

    Raises:
        HTTPException(401): 토큰이 없거나 유효하지 않은 경우
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="인증이 필요합니다. Authorization 헤더에 Bearer 토큰을 포함해주세요.",
        )

    payload = verify_token(credentials.credentials)
    return payload.user_id


async def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> str | None:
    """
    JWT 토큰을 선택적으로 검증합니다.

    토큰이 있으면 검증 후 사용자 ID를 반환하고,
    토큰이 없으면 None을 반환합니다.

    Args:
        credentials: HTTP Bearer 토큰 (없을 수 있음)

    Returns:
        str | None: 사용자 ID (비로그인 시 None)
    """
    if not credentials:
        return None

    try:
        payload = verify_token(credentials.credentials)
        return payload.user_id
    except HTTPException as exc:
        logger.warning(
            "선택 인증 토큰 검증 실패(v2): %s",
            exc.detail,
        )
        return None


async def get_redis_client() -> aioredis.Redis:
    """
    Redis 비동기 클라이언트를 주입합니다.

    Returns:
        aioredis.Redis: Redis 클라이언트

    Raises:
        RuntimeError: Redis가 초기화되지 않은 경우
    """
    return await get_redis()


async def get_redis_client_optional() -> aioredis.Redis | None:
    """
    Redis 비동기 클라이언트를 선택적으로 주입합니다.

    Redis가 초기화되지 않았으면 None을 반환해 호출부가 캐시 없이 동작할 수 있게 합니다.
    """
    try:
        return await get_redis()
    except RuntimeError:
        return None
