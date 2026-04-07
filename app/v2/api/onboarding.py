"""
회원 개인화 초기 설정(온보딩) API 엔드포인트 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 onboarding.py를 aiomysql Connection 기반으로 재구현합니다.
엔드포인트 구조와 응답 스키마는 v1과 완전히 동일합니다.

변경점: Depends(get_db) → Depends(get_conn)
"""

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.v2.api.deps import get_conn, get_current_user, get_redis_client
from app.model.schema import (
    GenreListResponse,
    GenreSelectionRequest,
    GenreSelectionResponse,
    MoodListResponse,
    MoodSelectionRequest,
    MoodSelectionResponse,
    OnboardingStatusResponse,
    WorldcupBracketResponse,
    WorldcupResultResponse,
    WorldcupSelectionRequest,
    WorldcupSelectionResponse,
)
from app.v2.service.onboarding_service import OnboardingService
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
    user_id: str = Depends(get_current_user),
):
    """장르 선택 저장 엔드포인트"""
    service = OnboardingService(conn)
    return await service.save_genre_selection(user_id, request.selected_genres)


# =========================================
# 2단계: 이상형 월드컵
# =========================================

@router.get(
    "/worldcup",
    response_model=WorldcupBracketResponse,
    summary="월드컵용 영화 후보 생성",
    description=(
        "선택한 장르 기반으로 16강 또는 32강 영화 후보를 생성하고 대진표를 반환합니다."
    ),
)
async def generate_worldcup(
    round_size: int = Query(
        default=16,
        description="라운드 크기 (16 또는 32)",
        ge=4,
        le=32,
    ),
    conn: aiomysql.Connection = Depends(get_conn),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 대진표 생성 엔드포인트"""
    if round_size > 16:
        round_size = 32
    else:
        round_size = 16

    service = WorldcupService(conn, redis)
    try:
        return await service.generate_bracket(user_id, round_size)
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
    return await service.submit_round(user_id, request)


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
    summary="온보딩 완료 여부 확인",
    description="3단계(장르→월드컵→무드) 전체 완료 여부를 단계별로 반환합니다.",
)
async def get_onboarding_status(
    conn: aiomysql.Connection = Depends(get_conn),
    user_id: str = Depends(get_current_user),
):
    """온보딩 상태 확인 엔드포인트"""
    service = OnboardingService(conn)
    return await service.get_onboarding_status(user_id)
