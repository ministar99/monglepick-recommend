"""
회원 개인화 초기 설정(온보딩) API 엔드포인트 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 onboarding.py를 aiomysql Connection 기반으로 재구현합니다.
엔드포인트 구조와 응답 스키마는 v1과 동일하며,
월드컵 시작 흐름은 `categories -> options -> start -> pick -> result` 순서를 따릅니다.

변경점: Depends(get_db) → Depends(get_conn)
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status

from app.v2.api.deps import get_conn, get_current_user, get_redis_client, get_redis_client_optional
from app.model.schema import (
    GenreListResponse,
    GenreSelectionRequest,
    GenreSelectionResponse,
    MoodListResponse,
    MoodSelectionRequest,
    MoodSelectionResponse,
    OnboardingStatusResponse,
    WorldcupBracketResponse,
    WorldcupCategoryOptionResponse,
    WorldcupGenreOptionResponse,
    WorldcupResultResponse,
    WorldcupSelectionRequest,
    WorldcupSelectionResponse,
    WorldcupStartOptionsRequest,
    WorldcupStartOptionsResponse,
    WorldcupStartRequest,
)
from app.v2.service.onboarding_service import OnboardingService
from app.v2.service.personalized_refresh_service import PersonalizedRefreshService
from app.v2.service.worldcup_service import WorldcupService

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라우터 정의
# ─────────────────────────────────────────
router = APIRouter(prefix="/onboarding", tags=["온보딩 (v2 Raw SQL)"])


# =========================================
# 1단계: 장르 선택
# =========================================

@router.get(
    "/genres",
    response_model=GenreListResponse,
    summary="장르 목록 + 대표 영화 포스터",
    description=(
        "온보딩 1단계용 장르 목록을 반환합니다. "
        "각 장르별로 대표 영화 5편의 포스터를 포함합니다."
    ),
)
async def get_genres(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """장르 목록 조회 엔드포인트"""
    service = OnboardingService(conn)
    return await service.get_genres_with_movies()


@router.post(
    "/genres",
    response_model=GenreSelectionResponse,
    summary="호감 장르 선택 저장",
    description="사용자가 선택한 호감 장르를 저장합니다. 최소 3개 이상 선택해야 합니다.",
)
async def save_genre_selection(
    request: GenreSelectionRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis | None = Depends(get_redis_client_optional),
    user_id: str = Depends(get_current_user),
):
    """장르 선택 저장 엔드포인트"""
    service = OnboardingService(conn)
    response = await service.save_genre_selection(user_id, request.selected_genres)
    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=10,
        reason="onboarding_genres",
        redis_client=redis,
    )
    return response


# =========================================
# 2단계: 이상형 월드컵
# =========================================

@router.get(
    "/worldcup/genres",
    response_model=list[WorldcupGenreOptionResponse],
    summary="커스텀 월드컵 장르 목록",
    description="genre_master 기반으로 커스텀 월드컵에서 선택 가능한 장르 목록을 반환합니다.",
)
async def get_worldcup_genres(
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """커스텀 월드컵 빌더용 장르 목록 조회 엔드포인트."""
    service = WorldcupService(conn, redis)
    return await service.get_available_genres()


@router.get(
    "/worldcup/categories",
    response_model=list[WorldcupCategoryOptionResponse],
    summary="월드컵 카테고리 목록",
    description="사용자에게 노출할 활성 월드컵 카테고리와 각 카테고리별 가능 라운드를 반환합니다.",
)
async def get_worldcup_categories(
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 시작 화면용 카테고리 목록 조회 엔드포인트."""
    service = WorldcupService(conn, redis)
    return await service.get_available_categories()


@router.post(
    "/worldcup/options",
    response_model=WorldcupStartOptionsResponse,
    summary="월드컵 시작 가능 라운드 계산",
    description=(
        "카테고리 또는 장르 조건으로 후보 풀 크기와 시작 가능한 라운드 목록을 계산합니다."
    ),
)
async def get_worldcup_start_options(
    request: WorldcupStartOptionsRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 시작 전 옵션 계산 엔드포인트."""
    service = WorldcupService(conn, redis)
    try:
        return await service.get_start_options(request)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(
    "/worldcup/start",
    response_model=WorldcupBracketResponse,
    summary="월드컵 대진표 생성",
    description=(
        "category/sourceType/selectedGenres/roundSize 조건으로 월드컵 후보를 생성하고 대진표를 반환합니다."
    ),
)
async def start_worldcup(
    request: WorldcupStartRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """새 월드컵 시작 엔드포인트."""
    service = WorldcupService(conn, redis)
    try:
        return await service.start_worldcup(user_id, request)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(
    "/worldcup",
    response_model=WorldcupSelectionResponse,
    summary="월드컵 라운드별 선택 결과 제출",
    description=(
        "각 매치에서 선택한 영화 ID를 제출합니다. "
        "결승이면 월드컵이 종료되고 결과가 저장됩니다."
    ),
)
async def submit_worldcup_round(
    request: WorldcupSelectionRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 라운드 결과 제출 엔드포인트"""
    service = WorldcupService(conn, redis)
    response = await service.submit_round(user_id, request)
    if response.next_round is None:
        await PersonalizedRefreshService.mark_dirty(
            user_id=user_id,
            limit=10,
            reason="worldcup",
            redis_client=redis,
        )
    return response


@router.get(
    "/worldcup/result",
    response_model=WorldcupResultResponse,
    summary="월드컵 결과 분석",
    description=(
        "월드컵 우승/준우승 영화와 장르별 선호도 레이더 차트 데이터를 반환합니다."
    ),
)
async def get_worldcup_result(
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 결과 조회 엔드포인트"""
    service = WorldcupService(conn, redis)
    try:
        return await service.get_result(user_id)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


# =========================================
# 3단계: 무드 선택
# =========================================

@router.get(
    "/moods",
    response_model=MoodListResponse,
    summary="무드 태그 목록",
    description="사용 가능한 무드 태그 목록을 반환합니다. (14개)",
)
async def get_moods(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """무드 태그 목록 조회 엔드포인트"""
    service = OnboardingService(conn)
    return await service.get_moods()


@router.post(
    "/moods",
    response_model=MoodSelectionResponse,
    summary="무드 기반 초기 설정 저장",
    description="사용자가 선택한 무드 태그를 저장합니다.",
)
async def save_mood_selection(
    request: MoodSelectionRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """무드 선택 저장 엔드포인트"""
    service = OnboardingService(conn)
    return await service.save_mood_selection(user_id, request.selected_moods)


# =========================================
# 온보딩 상태 확인
# =========================================

@router.get(
    "/status",
    response_model=OnboardingStatusResponse,
    summary="시작 미션 상태 확인",
    description="영화 월드컵, 선호 장르, 최애 영화 3개 미션의 완료 여부를 반환합니다.",
)
async def get_onboarding_status(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """시작 미션 온보딩 상태 확인 엔드포인트"""
    service = OnboardingService(conn)
    return await service.get_onboarding_status(user_id)
