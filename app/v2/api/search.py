"""
영화 검색 API 엔드포인트 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 search.py를 aiomysql Connection 기반으로 재구현합니다.
엔드포인트 구조와 응답 스키마는 v1과 완전히 동일합니다.

변경점: Depends(get_db) → Depends(get_conn)
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.v2.api.deps import get_conn, get_current_user, get_current_user_optional, get_redis_client
from app.model.schema import (
    AutocompleteResponse,
    MovieDetailResponse,
    MovieSearchResponse,
    RecentSearchResponse,
    SearchClickLogRequest,
    SearchClickLogResponse,
    TrendingResponse,
)
from app.search_genre_catalog import normalize_search_genre_labels
from app.v2.service.autocomplete_service import AutocompleteService
from app.v2.service.search_service import SearchService
from app.v2.service.trending_service import TrendingService

logger = logging.getLogger(__name__)

# ───────────────────────────────��─────────
# 라우터 정의
# ─────────────────────────────────────────
router = APIRouter(prefix="/search", tags=["영화 검색 (v2 Raw SQL)"])


@router.get(
    "/movies",
    response_model=MovieSearchResponse,
    summary="영화 검색",
    description=(
        "제목/감독/배우로 영화를 검색합니다. "
        "장르, 연도, 평점 필터와 정렬 옵션을 지원합니다."
    ),
)
async def search_movies(
    # 검색 키워드 (선택)
    q: str | None = Query(default=None, description="검색 키워드 (제목/감독/배우)"),
    # 검색 대상 타입
    search_type: str = Query(
        default="title",
        description="검색 대상 ('title', 'director', 'actor', 'all')",
        pattern="^(title|director|actor|all)$",
    ),
    # 장르 필터
    genre: str | None = Query(default=None, description="장르 필터 (예: 액션)"),
    genres: str | None = Query(
        default=None,
        description="장르 다중 선택 검색용 라벨 목록 (쉼표 구분, 예: 액션,드라마)",
    ),
    # 연도 범위
    year_from: int | None = Query(default=None, description="시작 연도 (포함)", ge=1900, le=2030),
    year_to: int | None = Query(default=None, description="끝 연도 (포함)", ge=1900, le=2030),
    # 평점 범위
    rating_min: float | None = Query(default=None, description="최소 평점 (포함)", ge=0.0, le=10.0),
    rating_max: float | None = Query(default=None, description="최대 평점 (포함)", ge=0.0, le=10.0),
    # 정렬
    sort_by: str = Query(
        default="relevance",
        description="정렬 기준 ('relevance', 'rating', 'release_date', 'title')",
        pattern="^(relevance|rating|release_date|title)$",
    ),
    sort_order: str = Query(
        default="desc",
        description="정렬 방향 ('asc', 'desc')",
        pattern="^(asc|desc)$",
    ),
    # 페이지네이션
    page: int = Query(default=1, description="페이지 번호 (1부터)", ge=1),
    size: int = Query(default=20, description="페이지 크기 (최대 100)", ge=1, le=100),
    # 의존성 — v2: aiomysql.Connection
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str | None = Depends(get_current_user_optional),
):
    """
    영화 검색 엔드포인트

    비로그인 사용자도 검색 가능하며, 로그인 시 검색 이력이 자동 저장됩니다.
    """
    normalized_genres = normalize_search_genre_labels(
        [item for item in (genres or "").split(",") if item.strip()]
    )
    service = SearchService(conn, redis)
    return await service.search_movies(
        keyword=q,
        search_type=search_type,
        genre=genre,
        genres=normalized_genres,
        year_from=year_from,
        year_to=year_to,
        rating_min=rating_min,
        rating_max=rating_max,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        size=size,
        user_id=user_id,
    )


@router.post(
    "/click",
    response_model=SearchClickLogResponse,
    summary="검색 결과 클릭 로그 저장",
    description="검색 결과 목록에서 사용자가 클릭한 영화를 저장합니다.",
)
async def log_search_click(
    payload: SearchClickLogRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str | None = Depends(get_current_user_optional),
):
    """검색 결과 클릭 이벤트 저장 엔드포인트 (v2 Raw SQL)"""
    service = SearchService(conn)
    return await service.log_search_click(
        user_id=user_id,
        keyword=payload.keyword,
        clicked_movie_id=payload.clicked_movie_id,
        result_count=payload.result_count,
        filters=payload.filters,
    )


@router.get(
    "/movies/{movie_id}",
    response_model=MovieDetailResponse,
    summary="영화 상세 조회",
    description="영화 ID로 상세 정보를 조회합니다.",
)
async def get_movie_detail(
    movie_id: str,
    conn: aiomysql.Connection = Depends(get_conn),
):
    """영화 상세 조회 엔드포인트 (인증 없이 접근 가능)"""
    service = SearchService(conn)
    try:
        return await service.get_movie_detail(movie_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/autocomplete",
    response_model=AutocompleteResponse,
    summary="검색어 자동완성",
    description="입력 중인 키워드에 대한 자동완성 후보를 반환합니다. (최대 10건, Redis 캐시 5분)",
)
async def autocomplete(
    q: str = Query(description="입력 중인 검색어 (최소 1글자)", min_length=1),
    limit: int = Query(default=10, description="최대 후보 수", ge=1, le=20),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
):
    """자동완성 엔드포인트 (인증 불필요)"""
    service = AutocompleteService(conn, redis)
    return await service.get_suggestions(prefix=q, limit=limit)


@router.get(
    "/trending",
    response_model=TrendingResponse,
    summary="인기 검색어 TOP 10",
    description="실시간 인기 검색어를 순위와 함께 반환합니다.",
)
async def get_trending(
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
):
    """인기 검색어 엔드포인트 (인증 불필요)"""
    service = TrendingService(conn, redis)
    return await service.get_trending()


@router.get(
    "/recent",
    response_model=RecentSearchResponse,
    summary="내 최근 검색어",
    description="로그인 사용자의 최근 검색 이력을 반환합니다. (최대 20건, 최신순)",
)
async def get_recent_searches(
    offset: int = Query(default=0, description="중복 제거된 목록 기준 시작 위치", ge=0),
    limit: int = Query(default=10, description="페이지당 조회 개수 (최대 10건)", ge=1, le=10),
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """최근 검색어 조회 엔드포인트 (로그인 필수)"""
    service = SearchService(conn)
    return await service.get_recent_searches(user_id, offset=offset, limit=limit)


@router.delete(
    "/recent",
    summary="최근 검색어 전체 삭제",
    description="로그인 사용자의 모든 최근 검색 이력을 삭제합니다.",
)
async def delete_all_recent(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """최근 검색어 전체 삭제 엔드포인트 (로그인 필수)"""
    service = SearchService(conn)
    deleted_count = await service.delete_all_recent(user_id)
    return {"message": f"{deleted_count}건의 검색 이력이 삭제되었습니다."}


@router.delete(
    "/recent/{keyword}",
    summary="최근 검색어 개별 삭제",
    description="특정 검색어를 최근 검색 이력에서 삭제합니다.",
)
async def delete_recent_keyword(
    keyword: str,
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """최근 검색어 개별 삭제 엔드포인트 (로그인 필수)"""
    service = SearchService(conn)
    success = await service.delete_recent_keyword(user_id, keyword)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"검색어 '{keyword}'를 찾을 수 없습니다.",
        )
    return {"message": f"검색어 '{keyword}'가 삭제되었습니다."}
