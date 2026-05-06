"""
최애 영화 API 엔드포인트 (v2 Raw SQL)

마이페이지 선호 설정 탭의 최애 영화 전당 기능을 recommend에서 직접 처리합니다.
"""

from __future__ import annotations

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status

from app.model.schema import (
    FavoriteMovieListResponse,
    FavoriteMovieSaveRequest,
)
from app.v2.api.deps import get_conn, get_current_user, get_redis_client_optional
from app.v2.service.favorite_movie_service import FavoriteMovieService
from app.v2.service.personalized_refresh_service import PersonalizedRefreshService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/users/me", tags=["최애 영화 (v2 Raw SQL)"])


@router.get(
    "/favorite-movies",
    response_model=FavoriteMovieListResponse,
    summary="내 최애 영화 목록 조회",
)
async def get_favorite_movies(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> FavoriteMovieListResponse:
    """로그인 사용자의 최애 영화 목록을 priority 순으로 반환합니다."""
    service = FavoriteMovieService(conn)
    return await service.get_favorite_movies(user_id=user_id)


@router.put(
    "/favorite-movies",
    response_model=FavoriteMovieListResponse,
    summary="내 최애 영화 목록 저장",
)
async def save_favorite_movies(
    payload: FavoriteMovieSaveRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> FavoriteMovieListResponse:
    """모달에서 선택한 최애 영화 목록과 순서를 저장합니다."""
    service = FavoriteMovieService(conn)
    try:
        response = await service.save_favorite_movies(user_id=user_id, movie_ids=payload.movie_ids)
        await PersonalizedRefreshService.mark_dirty(
            user_id=user_id,
            limit=10,
            reason="favorite_movies",
            redis_client=redis,
        )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.put(
    "/favorite-movies/order",
    response_model=FavoriteMovieListResponse,
    summary="내 최애 영화 순서 저장",
)
async def reorder_favorite_movies(
    payload: FavoriteMovieSaveRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> FavoriteMovieListResponse:
    """기존 최애 영화의 priority 순서를 저장합니다."""
    service = FavoriteMovieService(conn)
    try:
        response = await service.reorder_favorite_movies(user_id=user_id, movie_ids=payload.movie_ids)
        await PersonalizedRefreshService.mark_dirty(
            user_id=user_id,
            limit=10,
            reason="favorite_movies",
            redis_client=redis,
        )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
