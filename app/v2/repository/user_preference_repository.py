"""
사용자 선호도 리포지토리 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 UserPreferenceRepository를 Raw SQL로 재구현합니다.
user_preferences + worldcup_results 테이블에 대한 읽기/쓰기를 담당합니다.

JSON 컬럼(preferred_genres, preferred_moods 등)은
json.dumps()로 직렬화하여 저장하고, 조회 시 DTO에서 json.loads()로 역직렬화합니다.
"""

import json
import logging
from datetime import datetime, timezone

import aiomysql

from app.v2.model.dto import UserPreferenceDTO, WorldcupResultDTO

logger = logging.getLogger(__name__)


class UserPreferenceRepository:
    """사용자 선호도 CRUD 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        """
        Args:
            conn: aiomysql 비동기 커넥션
        """
        self._conn = conn

    # ─────────────────────────────────────────
    # user_preferences 테이블 (Spring Boot 공유)
    # ─────────────────────────────────────────

    async def get_by_user_id(self, user_id: str) -> UserPreferenceDTO | None:
        """
        사용자의 선호도를 조회합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            UserPreferenceDTO 또는 None (아직 설정되지 않은 경우)
        """
        sql = "SELECT * FROM user_preferences WHERE user_id = %s"
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            row = await cur.fetchone()

        return UserPreferenceDTO(**row) if row else None

    async def save_genres(self, user_id: str, genres: list[str]) -> UserPreferenceDTO:
        """
        사용자의 선호 장르를 저장합니다.

        기존 레코드가 있으면 preferred_genres만 갱신하고,
        없으면 새로 생성합니다.

        Args:
            user_id: 사용자 ID
            genres: 선호 장르 리스트 (예: ["액션", "SF", "스릴러"])

        Returns:
            저장/갱신된 UserPreferenceDTO
        """
        genres_json = json.dumps(genres, ensure_ascii=False)
        existing = await self.get_by_user_id(user_id)

        if existing:
            # 기존 레코드: preferred_genres만 갱신
            update_sql = (
                "UPDATE user_preferences SET preferred_genres = %s "
                "WHERE id = %s"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(update_sql, (genres_json, existing.id))

            existing.preferred_genres = genres
            return existing
        else:
            # 새 레코드: INSERT
            insert_sql = (
                "INSERT INTO user_preferences (user_id, preferred_genres) "
                "VALUES (%s, %s)"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(insert_sql, (user_id, genres_json))
                new_id = cur.lastrowid

            return UserPreferenceDTO(
                id=new_id,
                user_id=user_id,
                preferred_genres=genres,
            )

    async def save_moods(self, user_id: str, moods: list[str]) -> UserPreferenceDTO:
        """
        사용자의 선호 무드를 저장합니다.

        기존 레코드가 있으면 preferred_moods만 갱신합니다.

        Args:
            user_id: 사용자 ID
            moods: 선호 무드 리스트 (예: ["긴장감있는", "감동적인"])

        Returns:
            저장/갱신된 UserPreferenceDTO
        """
        moods_json = json.dumps(moods, ensure_ascii=False)
        existing = await self.get_by_user_id(user_id)

        if existing:
            # 기존 레코드: preferred_moods만 갱신
            update_sql = (
                "UPDATE user_preferences SET preferred_moods = %s "
                "WHERE id = %s"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(update_sql, (moods_json, existing.id))

            existing.preferred_moods = moods
            return existing
        else:
            # 새 레코드: INSERT
            insert_sql = (
                "INSERT INTO user_preferences (user_id, preferred_moods) "
                "VALUES (%s, %s)"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(insert_sql, (user_id, moods_json))
                new_id = cur.lastrowid

            return UserPreferenceDTO(
                id=new_id,
                user_id=user_id,
                preferred_moods=moods,
            )

    # ─────────────────────────────────────────
    # worldcup_results 테이블 (이 서비스 소유)
    # ─────────────────────────────────────────

    async def get_worldcup_result(self, user_id: str) -> WorldcupResultDTO | None:
        """
        사용자의 최신 월드컵 결과를 조회합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            WorldcupResultDTO 또는 None
        """
        sql = (
            "SELECT * FROM worldcup_results "
            "WHERE user_id = %s "
            "ORDER BY created_at DESC "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            row = await cur.fetchone()

        return WorldcupResultDTO(**row) if row else None

    async def save_worldcup_result(
        self,
        user_id: str,
        round_size: int,
        winner_movie_id: str,
        runner_up_movie_id: str | None,
        semi_final_movie_ids: list[str] | None,
        selection_log: dict | None,
        genre_preferences: dict[str, float] | None,
    ) -> WorldcupResultDTO:
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
            저장된 WorldcupResultDTO
        """
        now = datetime.now(timezone.utc)

        # JSON 직렬화 (v1에서는 ORM이 자동 처리했던 부분)
        semi_json = json.dumps(semi_final_movie_ids) if semi_final_movie_ids else None
        log_json = json.dumps(selection_log, ensure_ascii=False) if selection_log else None
        prefs_json = json.dumps(genre_preferences, ensure_ascii=False) if genre_preferences else None

        insert_sql = (
            "INSERT INTO worldcup_results "
            "(user_id, round_size, winner_movie_id, runner_up_movie_id, "
            "semi_final_movie_ids, selection_log, genre_preferences, "
            "onboarding_completed, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(insert_sql, (
                user_id, round_size, winner_movie_id, runner_up_movie_id,
                semi_json, log_json, prefs_json,
                True, now,
            ))
            new_id = cur.lastrowid

        return WorldcupResultDTO(
            id=new_id,
            user_id=user_id,
            round_size=round_size,
            winner_movie_id=winner_movie_id,
            runner_up_movie_id=runner_up_movie_id,
            semi_final_movie_ids=semi_json,
            selection_log=log_json,
            genre_preferences=prefs_json,
            onboarding_completed=True,
            created_at=now,
        )

    async def is_onboarding_completed(self, user_id: str) -> dict[str, bool]:
        """
        사용자의 온보딩 완료 여부를 단계별로 확인합니다.

        Returns:
            {
                "genre_selected": bool,
                "worldcup_completed": bool,
                "mood_selected": bool,
                "is_completed": bool,
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
