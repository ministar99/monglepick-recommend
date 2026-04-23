"""
회원 개인화 초기 설정(온보딩) API 엔드포인트

REQ_016: 초기 장르 선택 (최소 3개, 대표 영화 포스터 표시)
REQ_017: 영화 이상형 월드컵 (16강/32강 토너먼트)
REQ_018: 월드컵 결과 → 장르 선호도 분석 (레이더 차트)
REQ_019: 무드 기반 초기 영화 추천 설정

온보딩 3단계 흐름:
1. 장르 선택 (GET/POST /genres)
2. 이상형 월드컵
   - GET  /worldcup/categories
   - POST /worldcup/options
   - POST /worldcup/start
   - POST /worldcup
   - GET  /worldcup/result
3. 무드 선택 (GET/POST /moods)
4. 완료 확인 (GET /status)

모든 온보딩 엔드포인트는 로그인 필수(JWT 인증)입니다.
"""

import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_redis_client
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
from app.service.onboarding_service import OnboardingService
from app.service.worldcup_service import WorldcupService

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라우터 정의
# ─────────────────────────────────────────
router = APIRouter(prefix="/onboarding", tags=["온보딩 (개인화 초기 설정)"])


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
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user),
):
    """
    장르 목록 조회 엔드포인트

    DB에 존재하는 모든 장르와 각 장르의 대표 영화(평점 6.0+, 포스터 있음)를 반환합니다.
    프론트엔드에서 장르 카드 UI를 구성할 때 사용합니다.
    """
    service = OnboardingService(db)
    return await service.get_genres_with_movies()


@router.post(
    "/genres",
    response_model=GenreSelectionResponse,
    summary="호감 장르 선택 저장",
    description="사용자가 선택한 호감 장르를 저장합니다. 최소 3개 이상 선택해야 합니다.",
)
async def save_genre_selection(
    request: GenreSelectionRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user),
):
    """
    장르 선택 저장 엔드포인트

    user_preferences 테이블의 preferred_genres 컬럼에 JSON 배열로 저장합니다.
    이 데이터는 이상형 월드컵 후보 선정의 기반이 됩니다.
    """
    service = OnboardingService(db)
    return await service.save_genre_selection(user_id, request.selected_genres)


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
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """커스텀 월드컵 빌더용 장르 목록 조회 엔드포인트."""
    service = WorldcupService(db, redis)
    return await service.get_available_genres()


@router.get(
    "/worldcup/categories",
    response_model=list[WorldcupCategoryOptionResponse],
    summary="월드컵 카테고리 목록",
    description="사용자에게 노출할 활성 월드컵 카테고리와 각 카테고리별 가능 라운드를 반환합니다.",
)
async def get_worldcup_categories(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 시작 화면용 카테고리 목록 조회 엔드포인트."""
    service = WorldcupService(db, redis)
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
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """월드컵 시작 전 옵션 계산 엔드포인트."""
    service = WorldcupService(db, redis)
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
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """새 월드컵 시작 엔드포인트."""
    service = WorldcupService(db, redis)
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
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """
    월드컵 라운드 결과 제출 엔드포인트

    클라이언트에서 한 라운드가 끝날 때마다 호출합니다.
    결승전(is_final=True 또는 selections 1개)이면 결과를 DB에 저장하고
    장르 선호도 분석을 수행합니다.
    """
    service = WorldcupService(db, redis)
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
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis_client),
    user_id: str = Depends(get_current_user),
):
    """
    월드컵 결과 조회 엔드포인트

    월드컵 완료 후 호출합니다.
    장르별 선호도 점수(0.0~1.0)를 레이더 차트 데이터로 반환합니다.
    """
    service = WorldcupService(db, redis)
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
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user),
):
    """
    무드 태그 목록 조회 엔드포인트

    온보딩 3단계에서 무드 선택 UI에 표시할 태그 목록을 반환합니다.
    각 태그에는 이름과 대표 이모지가 포함됩니다.
    """
    service = OnboardingService(db)
    return await service.get_moods()


@router.post(
    "/moods",
    response_model=MoodSelectionResponse,
    summary="무드 기반 초기 설정 저장",
    description="사용자가 선택한 무드 태그를 저장합니다.",
)
async def save_mood_selection(
    request: MoodSelectionRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user),
):
    """
    무드 선택 저장 엔드포인트

    user_preferences 테이블의 preferred_moods 컬럼에 JSON 배열로 저장합니다.
    이것이 온보딩의 마지막 단계입니다.
    """
    service = OnboardingService(db)
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
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user),
):
    """
    온보딩 상태 확인 엔드포인트

    프론트엔드의 시작 미션 허브(`/onboarding`)에서
    체크 상태와 진행 개수를 그릴 때 사용합니다.
    """
    service = OnboardingService(db)
    return await service.get_onboarding_status(user_id)
