"""
사용자 선호도 리포지토리

user_preferences 테이블에 대한 읽기/쓰기를 담당합니다.
DDL 기준: init.sql의 user_preferences (JSON 컬럼) + worldcup_results

온보딩 결과(장르 선택, 월드컵 분석, 무드 선택)를 저장합니다.
JSON 컬럼(preferred_genres, preferred_moods 등)은
파이썬 리스트를 직접 할당하면 SQLAlchemy가 자동 직렬화합니다.

worldcup_results 테이블은 이 서비스가 소유합니다.
"""

import json
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import UserPreference, WorldcupResult


class UserPreferenceRepository:
    """사용자 선호도 CRUD 리포지토리"""

    def __init__(self, session: AsyncSession):
        """
        Args:
            session: SQLAlchemy 비동기 세션
        """
        self._session = session

    # ─────────────────────────────────────────
    # user_preferences 테이블 (Spring Boot 공유)
    # ─────────────────────────────────────────

    async def get_by_user_id(self, user_id: str) -> UserPreference | None:
        """
        사용자의 선호도를 조회합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            UserPreference 엔티티 또는 None (아직 설정되지 않은 경우)
        """
        result = await self._session.execute(
            select(UserPreference).where(UserPreference.user_id == user_id)
        )
        return result.scalar_one_or_none()

    async def save_genres(self, user_id: str, genres: list[str]) -> UserPreference:
        """
        사용자의 선호 장르를 저장합니다.

        기존 레코드가 있으면 preferred_genres만 갱신하고,
        없으면 새로 생성합니다.

        Args:
            user_id: 사용자 ID
            genres: 선호 장르 리스트 (예: ["액션", "SF", "스릴러"])

        Returns:
            저장/갱신된 UserPreference 엔티티
        """
        existing = await self.get_by_user_id(user_id)
        if existing:
            # JSON 컬럼: 파이썬 리스트를 직접 할당 (SQLAlchemy가 자동 직렬화)
            existing.preferred_genres = genres
            self._session.add(existing)
        else:
            existing = UserPreference(
                user_id=user_id,
                preferred_genres=genres,
            )
            self._session.add(existing)

        await self._session.flush()
        return existing

    async def save_moods(self, user_id: str, moods: list[str]) -> UserPreference:
        """
        사용자의 선호 무드를 저장합니다.

        기존 레코드가 있으면 preferred_moods만 갱신합니다.

        Args:
            user_id: 사용자 ID
            moods: 선호 무드 리스트 (예: ["긴장감있는", "감동적인"])

        Returns:
            저장/갱신된 UserPreference 엔티티
        """
        existing = await self.get_by_user_id(user_id)
        if existing:
            # JSON 컬럼: 파이썬 리스트를 직접 할당 (SQLAlchemy가 자동 직렬화)
            existing.preferred_moods = moods
            self._session.add(existing)
        else:
            existing = UserPreference(
                user_id=user_id,
                preferred_moods=moods,
            )
            self._session.add(existing)

        await self._session.flush()
        return existing

    # ─────────────────────────────────────────
    # worldcup_results 테이블 (이 서비스 소유)
    # ─────────────────────────────────────────

    async def get_worldcup_result(self, user_id: str) -> WorldcupResult | None:
        """
        사용자의 월드컵 결과를 조회합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            WorldcupResult 엔티티 또는 None
        """
        result = await self._session.execute(
            select(WorldcupResult)
            .where(WorldcupResult.user_id == user_id)
            .order_by(WorldcupResult.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def save_worldcup_result(
        self,
        user_id: str,
        round_size: int,
        winner_movie_id: str,
        runner_up_movie_id: str | None,
        semi_final_movie_ids: list[str] | None,
        selection_log: dict | None,
        genre_preferences: dict[str, float] | None,
    ) -> WorldcupResult:
        """
        월드컵 결과를 저장합니다.

        Args:
            user_id: 사용자 ID
            round_size: 라운드 크기 (16 또는 32)
            winner_movie_id: 우승 영화 ID
            runner_up_movie_id: 준우승 영화 ID
            semi_final_movie_ids: 4강 영화 ID 목록
            selection_log: 라운드별 선택 로그
            genre_preferences: 분석된 장르 선호도

        Returns:
            저장된 WorldcupResult 엔티티
        """
        worldcup = WorldcupResult(
            user_id=user_id,
            round_size=round_size,
            winner_movie_id=winner_movie_id,
            runner_up_movie_id=runner_up_movie_id,
            semi_final_movie_ids=(
                json.dumps(semi_final_movie_ids) if semi_final_movie_ids else None
            ),
            selection_log=(
                json.dumps(selection_log, ensure_ascii=False) if selection_log else None
            ),
            genre_preferences=(
                json.dumps(genre_preferences, ensure_ascii=False)
                if genre_preferences
                else None
            ),
            onboarding_completed=True,
            created_at=datetime.now(timezone.utc),
        )
        self._session.add(worldcup)
        await self._session.flush()
        return worldcup

    async def is_onboarding_completed(self, user_id: str) -> dict[str, bool]:
        """
        사용자의 온보딩 완료 여부를 단계별로 확인합니다.

        Returns:
            {
                "genre_selected": bool,  # 장르 선택 완료
                "worldcup_completed": bool,  # 월드컵 완료
                "mood_selected": bool,  # 무드 선택 완료
                "is_completed": bool,  # 전체 완료
            }
        """
        # user_preferences에서 장르/무드 확인
        pref = await self.get_by_user_id(user_id)
        genre_selected = bool(pref and pref.preferred_genres)
        mood_selected = bool(pref and pref.preferred_moods)

        # worldcup_results에서 월드컵 완료 확인
        worldcup = await self.get_worldcup_result(user_id)
        worldcup_completed = bool(worldcup and worldcup.onboarding_completed)

        return {
            "genre_selected": genre_selected,
            "worldcup_completed": worldcup_completed,
            "mood_selected": mood_selected,
            "is_completed": genre_selected and worldcup_completed and mood_selected,
        }
