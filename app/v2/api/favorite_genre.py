"""
선호 장르 API 엔드포인트 (v2 Raw SQL)

마이페이지 선호 설정 탭의 선호 장르 조회/저장을 recommend에서 직접 처리합니다.
"""

from __future__ import annotations

import aiomysql
from fastapi import APIRouter, Depends, HTTPException, status

from app.model.schema import FavoriteGenreListResponse, FavoriteGenreSaveRequest
from app.v2.api.deps import get_conn, get_current_user
from app.v2.service.favorite_genre_service import FavoriteGenreService

router = APIRouter(prefix="/users/me", tags=["선호 장르 (v2 Raw SQL)"])


@router.get(
    "/favorite-genres",
    response_model=FavoriteGenreListResponse,
    summary="내 선호 장르 설정 조회",
)
async def get_favorite_genres(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> FavoriteGenreListResponse:
    """로그인 사용자의 선호 장르 설정 화면 데이터를 반환합니다."""
    service = FavoriteGenreService(conn)
    return await service.get_favorite_genres(user_id=user_id)


@router.put(
    "/favorite-genres",
    response_model=FavoriteGenreListResponse,
    summary="내 선호 장르 저장",
)
async def save_favorite_genres(
    payload: FavoriteGenreSaveRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> FavoriteGenreListResponse:
    """사용자가 선택한 선호 장르 목록과 순서를 저장합니다."""
    service = FavoriteGenreService(conn)
    try:
        return await service.save_favorite_genres(user_id=user_id, genre_ids=payload.genre_ids)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
