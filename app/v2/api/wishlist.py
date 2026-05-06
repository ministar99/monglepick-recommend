"""
위시리스트 API 엔드포인트 (v2 Raw SQL)

Backend의 /users/me/wishlist 계열 기능을 recommend(FastAPI)로 이관한다.
마이페이지 탭과 영화 상세의 위시리스트 버튼이 이 라우터를 사용한다.
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Path, Query

from app.model.schema import (
    WishlistListResponse,
    WishlistStatusResponse,
    WishlistToggleResponse,
)
from app.v2.api.deps import get_conn, get_current_user, get_redis_client_optional
from app.v2.service.personalized_refresh_service import PersonalizedRefreshService
from app.v2.service.wishlist_service import WishlistService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/users/me", tags=["위시리스트 (v2 Raw SQL)"])


@router.get(
    "/wishlist",
    response_model=WishlistListResponse,
    summary="내 위시리스트 조회",
)
async def get_wishlist(
    page: int = Query(default=1, ge=1, description="페이지 번호"),
    size: int = Query(default=20, ge=1, le=100, description="페이지 크기"),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> WishlistListResponse:
    """로그인 사용자의 위시리스트를 조회한다."""
    service = WishlistService(conn)
    return await service.get_wishlist(user_id=user_id, page=page, size=size)


@router.get(
    "/wishlist/{movie_id}",
    response_model=WishlistStatusResponse,
    summary="특정 영화 위시리스트 포함 여부 조회",
)
async def get_wishlist_status(
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> WishlistStatusResponse:
    """영화 상세 진입 시 현재 영화의 위시리스트 포함 여부를 반환한다."""
    service = WishlistService(conn)
    return await service.get_wishlist_status(user_id=user_id, movie_id=movie_id)


@router.post(
    "/wishlist/{movie_id}",
    response_model=WishlistToggleResponse,
    summary="위시리스트 추가",
)
async def add_to_wishlist(
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> WishlistToggleResponse:
    """위시리스트에 영화를 추가한다."""
    service = WishlistService(conn)
    response = await service.add_to_wishlist(user_id=user_id, movie_id=movie_id)
    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=10,
        reason="wishlist",
        redis_client=redis,
    )
    return response


@router.delete(
    "/wishlist/{movie_id}",
    response_model=WishlistToggleResponse,
    summary="위시리스트 제거",
)
async def remove_from_wishlist(
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> WishlistToggleResponse:
    """위시리스트에서 영화를 제거한다."""
    service = WishlistService(conn)
    response = await service.remove_from_wishlist(user_id=user_id, movie_id=movie_id)
    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=10,
        reason="wishlist",
        redis_client=redis,
    )
    return response
