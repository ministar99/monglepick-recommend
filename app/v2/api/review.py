"""
리뷰 API 엔드포인트 (v2 Raw SQL)

영화 상세 화면의 리뷰 조회/작성/수정/삭제/좋아요 토글을
recommend(FastAPI)에서 직접 처리한다.
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query, status

from app.model.schema import (
    LikeResponse,
    ReviewCreateRequest,
    ReviewItem,
    ReviewListResponse,
    ReviewUpdateRequest,
    UserReviewListResponse,
)
from app.v2.api.deps import get_conn, get_current_user, get_current_user_optional, get_redis_client_optional
from app.v2.service.personalized_refresh_service import PersonalizedRefreshService
from app.v2.service.review_service import DuplicateReviewError, ReviewService

logger = logging.getLogger(__name__)

movie_review_router = APIRouter(prefix="/movies", tags=["리뷰 (v2 Raw SQL)"])
user_review_router = APIRouter(prefix="/users/me", tags=["리뷰 (v2 Raw SQL)"])


@movie_review_router.get(
    "/{movie_id}/reviews",
    response_model=ReviewListResponse,
    summary="영화별 리뷰 목록 조회",
)
async def get_reviews(
    movie_id: str = Path(..., description="영화 ID"),
    page: int = Query(default=1, ge=1, description="페이지 번호"),
    size: int = Query(default=20, ge=1, le=100, description="페이지 크기"),
    sort: str = Query(default="latest", pattern="^(latest|rating_high|rating_low)$"),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str | None = Depends(get_current_user_optional),
) -> ReviewListResponse:
    """특정 영화의 리뷰를 최신순 또는 평점순으로 조회한다."""
    service = ReviewService(conn)
    return await service.get_reviews(
        movie_id=movie_id,
        page=page,
        size=size,
        sort=sort,
        user_id=user_id,
    )


@movie_review_router.post(
    "/{movie_id}/reviews",
    response_model=ReviewItem,
    status_code=status.HTTP_201_CREATED,
    summary="리뷰 작성",
    responses={
        409: {"description": "이미 해당 영화에 리뷰를 작성한 경우"},
    },
)
async def create_review(
    payload: ReviewCreateRequest,
    background_tasks: BackgroundTasks,
    movie_id: str = Path(..., description="영화 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> ReviewItem:
    """
    영화 리뷰를 작성한다.

    "봤다 = 리뷰" 단일 진실 원본 원칙에 따라 1 유저 1 영화 1 리뷰만 허용한다.
    이미 작성한 영화에 재요청 시 HTTP 409 Conflict 로 응답한다
    (Backend `monglepick-backend` ReviewService 의 DUPLICATE_REVIEW 와 동일 정책).
    """
    service = ReviewService(conn)
    try:
        response = await service.create_review(movie_id=movie_id, payload=payload, user_id=user_id)
        should_refresh = await service.should_refresh_personalized_profile(
            user_id=user_id,
            rating=response.rating,
            exclude_review_id=response.id,
        )
        if should_refresh:
            await PersonalizedRefreshService.enqueue_refresh(
                user_id=user_id,
                limit=10,
                reason="review",
                background_tasks=background_tasks,
                redis_client=redis,
            )
        return response
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except DuplicateReviewError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@movie_review_router.put(
    "/{movie_id}/reviews/{review_id}",
    response_model=ReviewItem,
    summary="리뷰 수정",
)
async def update_review(
    payload: ReviewUpdateRequest,
    background_tasks: BackgroundTasks,
    movie_id: str = Path(..., description="영화 ID"),
    review_id: int = Path(..., description="리뷰 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> ReviewItem:
    """작성자 본인의 리뷰를 수정한다."""
    service = ReviewService(conn)
    try:
        response = await service.update_review(
            movie_id=movie_id,
            review_id=review_id,
            payload=payload,
            user_id=user_id,
        )
        should_refresh = await service.should_refresh_personalized_profile(
            user_id=user_id,
            rating=response.rating,
            exclude_review_id=review_id,
        )
        if should_refresh:
            await PersonalizedRefreshService.enqueue_refresh(
                user_id=user_id,
                limit=10,
                reason="review",
                background_tasks=background_tasks,
                redis_client=redis,
            )
        return response
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


@movie_review_router.delete(
    "/{movie_id}/reviews/{review_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="리뷰 삭제",
)
async def delete_review(
    background_tasks: BackgroundTasks,
    movie_id: str = Path(..., description="영화 ID"),
    review_id: int = Path(..., description="리뷰 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
) -> None:
    """작성자 본인의 리뷰를 삭제한다."""
    service = ReviewService(conn)
    try:
        await service.delete_review(movie_id=movie_id, review_id=review_id, user_id=user_id)
        await PersonalizedRefreshService.enqueue_refresh(
            user_id=user_id,
            limit=10,
            reason="review",
            background_tasks=background_tasks,
            redis_client=redis,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


@movie_review_router.post(
    "/{movie_id}/reviews/{review_id}/like",
    response_model=LikeResponse,
    summary="리뷰 좋아요 토글",
)
async def toggle_review_like(
    movie_id: str = Path(..., description="영화 ID"),
    review_id: int = Path(..., description="리뷰 ID"),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> LikeResponse:
    """리뷰 좋아요를 등록/취소한다."""
    service = ReviewService(conn)
    try:
        return await service.toggle_review_like(movie_id=movie_id, review_id=review_id, user_id=user_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@user_review_router.get(
    "/reviews",
    response_model=UserReviewListResponse,
    summary="내 리뷰 목록 조회",
)
async def get_my_reviews(
    page: int = Query(default=1, ge=1, description="페이지 번호"),
    size: int = Query(default=20, ge=1, le=100, description="페이지 크기"),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
) -> UserReviewListResponse:
    """마이페이지에서 사용할 현재 로그인 사용자의 리뷰 목록을 최신순으로 조회한다."""
    service = ReviewService(conn)
    return await service.get_user_reviews(user_id=user_id, page=page, size=size)
