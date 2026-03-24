"""
FastAPI 의존성 주입 모듈

모든 엔드포인트에서 공통으로 사용하는 의존성을 정의합니다:
- get_db: 비동기 DB 세션 (요청 단위 스코프)
- get_current_user: JWT 토큰 검증 후 사용자 ID 추출
- get_current_user_optional: 비로그인도 허용 (user_id=None)
- get_redis_client: Redis 비동기 클라이언트

사용법 (엔드포인트에서):
    @router.get("/movies")
    async def search(
        db: AsyncSession = Depends(get_db),
        user_id: int = Depends(get_current_user),
        redis: aioredis.Redis = Depends(get_redis_client),
    ):
        ...
"""

from collections.abc import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.core.redis import get_redis
from app.core.security import verify_token

# HTTP Bearer 토큰 추출기
# auto_error=False: 토큰이 없어도 에러를 발생시키지 않음 (optional auth에 사용)
_bearer_scheme = HTTPBearer(auto_error=False)
# auto_error=True: 토큰이 없으면 401 에러 발생 (required auth에 사용)
_bearer_scheme_required = HTTPBearer(auto_error=True)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    비동기 DB 세션을 주입합니다.

    요청 시작 시 세션을 생성하고, 요청 완료 시 자동으로 커밋/롤백/닫기를 수행합니다.

    Yields:
        AsyncSession: SQLAlchemy 비동기 세션
    """
    async for session in get_async_session():
        yield session


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme_required),
) -> str:
    """
    JWT 토큰을 검증하고 사용자 ID를 반환합니다.

    Authorization 헤더에서 Bearer 토큰을 추출하고,
    Spring Boot 백엔드와 동일한 시크릿으로 검증합니다.

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
    비로그인 사용자도 사용 가능한 엔드포인트에서 사용합니다.

    Args:
        credentials: HTTP Bearer 토큰 (없을 수 있음)

    Returns:
        str | None: 사용자 ID (비로그인 시 None, VARCHAR(50))
    """
    if not credentials:
        return None

    try:
        payload = verify_token(credentials.credentials)
        return payload.user_id
    except HTTPException:
        # 토큰이 있지만 유효하지 않으면 None 반환 (에러 발생 안 함)
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
