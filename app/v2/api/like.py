"""
영화 좋아요 API 엔드포인트 (v2 Raw SQL + Redis 하이브리드 캐시)
=================================================================

Backend(monglepick-backend) `domain/movie/LikeController`에서 이관된 엔드포인트다.
URL 경로·응답 스키마(`liked`, `likeCount`)까지 Backend와 1:1 동일하게 유지하여,
Nginx에서 `/api/v1/movies/{id}/like*` 경로만 이 FastAPI로 프록시해 주면
Frontend 수정 없이 이관이 완료된다.

경로 매핑:
  POST /api/v2/movies/{movie_id}/like         → 토글 (JWT 필수)
  GET  /api/v2/movies/{movie_id}/like         → 내 좋아요 상태 (JWT 필수)
  GET  /api/v2/movies/{movie_id}/like/count   → 전체 카운트 (공개)

정합성 모델:
  - Redis 캐시에 즉시 반영 → 사용자에게 빠른 응답
  - write-behind 스케줄러(app/background/like_flush.py)가 주기적으로 MySQL로 배치 반영
  - Redis 장애 시 DB 동기 폴백 (LikeService에서 자동 전환)

인증:
  - POST /like, GET /like 는 JWT 필수 (Backend와 동일)
  - GET /like/count 는 공개 — 비로그인 사용자도 조회 가능
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Path

from app.model.schema import LikeResponse
from app.v2.api.deps import (
    get_conn,
    get_current_user,
    get_redis_client,
)
from app.v2.service.like_service import LikeService

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라우터 정의
# URL은 Backend와 동일한 구조 (/movies/{movie_id}/like)
# ─────────────────────────────────────────
router = APIRouter(prefix="/movies", tags=["영화 좋아요 (v2 Raw SQL)"])


@router.post(
    "/{movie_id}/like",
    response_model=LikeResponse,
    summary="영화 좋아요 토글",
    description=(
        "영화 좋아요를 토글한다 (등록/취소/복구). "
        "Redis 캐시에 즉시 반영되고 실제 DB는 write-behind 스케줄러가 주기 반영한다. "
        "JWT 인증 필수."
    ),
)
async def toggle_like(
    movie_id: str = Path(..., description="영화 ID (VARCHAR 50, TMDB/KOBIS/KMDb)"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
) -> LikeResponse:
    """
    영화 좋아요 토글 엔드포인트.

    응답 스키마는 Backend LikeResponse와 완전히 동일:
      { "liked": bool, "likeCount": int }
    """
    logger.debug("[v2] 좋아요 토글 요청 user=%s movie=%s", user_id, movie_id)
    service = LikeService(conn, redis)
    return await service.toggle_like(user_id, movie_id)


@router.get(
    "/{movie_id}/like",
    response_model=LikeResponse,
    summary="내 영화 좋아요 상태 조회",
    description=(
        "로그인한 사용자가 해당 영화에 활성 좋아요를 눌렀는지 확인한다. "
        "응답에 전체 좋아요 수도 함께 포함된다. JWT 인증 필수."
    ),
)
async def get_my_like_status(
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
) -> LikeResponse:
    """
    현재 사용자의 좋아요 상태 조회.

    Backend `GET /api/v1/movies/{movieId}/like`와 동일한 응답.
    """
    logger.debug("[v2] 좋아요 상태 조회 user=%s movie=%s", user_id, movie_id)
    service = LikeService(conn, redis)
    return await service.is_liked(user_id, movie_id)


@router.get(
    "/{movie_id}/like/count",
    response_model=LikeResponse,
    summary="영화 좋아요 수 조회 (공개)",
    description=(
        "해당 영화의 전체 활성 좋아요 수만 반환한다. 비로그인 사용자도 접근 가능하며 "
        "Backend와 동일하게 `liked` 필드는 항상 false로 고정된다."
    ),
)
async def get_like_count(
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
) -> LikeResponse:
    """
    영화 좋아요 수 조회 (비로그인 허용).

    Backend `GET /api/v1/movies/{movieId}/like/count`와 동일한 응답.
    SecurityConfig에서 해당 경로를 permitAll로 설정하는 것과 동등한 처리.
    """
    logger.debug("[v2] 좋아요 수 조회 movie=%s", movie_id)
    service = LikeService(conn, redis)
    return await service.get_count(movie_id)
