"""
온보딩 서비스

REQ_016: 초기 장르 선택 (최소 3개) + 대표 영화 포스터
REQ_018: 월드컵 결과 → 장르 선호도 분석 (레이더 차트)
REQ_019: 무드 기반 초기 영화 추천 설정

온보딩 3단계 흐름:
1. 장르 선택 → user_preferences.preferred_genres 저장
2. 이상형 월드컵 → worldcup_results 테이블에 결과 저장 + 선호도 분석
3. 무드 선택 → user_preferences.preferred_moods 저장

모든 단계 완료 시 온보딩 완료(is_completed=True)로 표시합니다.
"""

import json
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.schema import (
    GenreListResponse,
    GenreSelectionResponse,
    GenreWithMovies,
    MoodListResponse,
    MoodSelectionResponse,
    MoodTag,
    MovieBrief,
    OnboardingStatusResponse,
)
from app.repository.movie_repository import MovieRepository
from app.repository.user_preference_repository import UserPreferenceRepository

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 사전 정의된 무드 태그 목록
# Neo4j에 21개 MoodTag가 있으나, 온보딩에서는 주요 14개만 사용
# ─────────────────────────────────────────
MOOD_TAGS: list[MoodTag] = [
    MoodTag(id=1, name="긴장감있는", emoji="😰"),
    MoodTag(id=2, name="감동적인", emoji="🥹"),
    MoodTag(id=3, name="유쾌한", emoji="😄"),
    MoodTag(id=4, name="로맨틱한", emoji="💕"),
    MoodTag(id=5, name="무서운", emoji="😱"),
    MoodTag(id=6, name="신비로운", emoji="✨"),
    MoodTag(id=7, name="잔잔한", emoji="🌊"),
    MoodTag(id=8, name="열혈", emoji="🔥"),
    MoodTag(id=9, name="슬픈", emoji="😢"),
    MoodTag(id=10, name="철학적인", emoji="🤔"),
    MoodTag(id=11, name="유머러스", emoji="🤣"),
    MoodTag(id=12, name="서정적인", emoji="🌸"),
    MoodTag(id=13, name="반전있는", emoji="😮"),
    MoodTag(id=14, name="몰입감있는", emoji="🎬"),
]


class OnboardingService:
    """온보딩 전체 흐름 관리 서비스"""

    def __init__(self, session: AsyncSession):
        """
        Args:
            session: SQLAlchemy 비동기 세션
        """
        self._session = session
        self._settings = get_settings()
        self._movie_repo = MovieRepository(session)
        self._pref_repo = UserPreferenceRepository(session)

    # ─────────────────────────────────────────
    # 1단계: 장르 선택
    # ─────────────────────────────────────────

    async def get_genres_with_movies(self) -> GenreListResponse:
        """
        전체 장르 목록과 각 장르의 대표 영화 포스터를 반환합니다.

        DB에서 고유 장르를 추출하고, 각 장르별로
        평점 높고 포스터가 있는 대표 영화 5편을 조회합니다.

        Returns:
            GenreListResponse: 장르별 대표 영화 목록
        """
        # DB에서 모든 고유 장르 추출
        all_genres = await self._movie_repo.get_all_genres()

        genres_with_movies: list[GenreWithMovies] = []
        for genre_name in all_genres:
            # 각 장르별 대표 영화 5편 조회 (평점 6.0 이상, 포스터 있는 것)
            movies = await self._movie_repo.find_by_genre(
                genre=genre_name,
                limit=5,
                min_rating=6.0,
            )

            # MovieBrief로 변환
            movie_briefs = [self._to_movie_brief(m) for m in movies]

            genres_with_movies.append(
                GenreWithMovies(
                    genre=genre_name,
                    representative_movies=movie_briefs,
                )
            )

        return GenreListResponse(genres=genres_with_movies)

    async def save_genre_selection(
        self, user_id: str, selected_genres: list[str]
    ) -> GenreSelectionResponse:
        """
        사용자의 호감 장르 선택을 저장합니다.

        user_preferences.preferred_genres에 JSON 배열로 저장합니다.
        최소 3개 이상 선택해야 합니다 (스키마에서 검증).

        Args:
            user_id: 사용자 ID
            selected_genres: 선택한 장르 목록

        Returns:
            GenreSelectionResponse: 저장 완료 응답
        """
        await self._pref_repo.save_genres(user_id, selected_genres)

        logger.info(f"장르 선택 저장 완료: user_id={user_id}, genres={selected_genres}")

        return GenreSelectionResponse(
            message="장르 선택이 저장되었습니다.",
            selected_genres=selected_genres,
        )

    # ─────────────────────────────────────────
    # 3단계: 무드 선택
    # ─────────────────────────────────────────

    async def get_moods(self) -> MoodListResponse:
        """
        사용 가능한 무드 태그 목록을 반환합니다.

        Returns:
            MoodListResponse: 무드 태그 목록 (14개)
        """
        return MoodListResponse(moods=MOOD_TAGS)

    async def save_mood_selection(
        self, user_id: str, selected_moods: list[str]
    ) -> MoodSelectionResponse:
        """
        사용자의 무드 선택을 저장합니다.

        user_preferences.preferred_moods에 JSON 배열로 저장합니다.

        Args:
            user_id: 사용자 ID
            selected_moods: 선택한 무드 태그 이름 목록

        Returns:
            MoodSelectionResponse: 저장 완료 응답
        """
        await self._pref_repo.save_moods(user_id, selected_moods)

        logger.info(f"무드 선택 저장 완료: user_id={user_id}, moods={selected_moods}")

        return MoodSelectionResponse(
            message="무드 설정이 저장되었습니다.",
            selected_moods=selected_moods,
        )

    # ─────────────────────────────────────────
    # 온보딩 상태 확인
    # ─────────────────────────────────────────

    async def get_onboarding_status(self, user_id: str) -> OnboardingStatusResponse:
        """
        사용자의 온보딩 완료 여부를 단계별로 확인합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            OnboardingStatusResponse: 단계별 완료 여부
        """
        status = await self._pref_repo.is_onboarding_completed(user_id)
        return OnboardingStatusResponse(**status)

    # ─────────────────────────────────────────
    # 유틸리티
    # ─────────────────────────────────────────

    def _to_movie_brief(self, movie) -> MovieBrief:
        """Movie 엔티티를 MovieBrief 스키마로 변환합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        return MovieBrief(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=movie.get_genres_list(),
            release_year=movie.release_year,
            rating=movie.rating,
            poster_url=poster_url,
            overview=movie.overview,
        )
