"""
월드컵 세션 리포지토리 (v2 Raw SQL)

recommend 런타임에서 worldcup_session 테이블을 생성/갱신합니다.
"""

import json
from datetime import datetime, timezone

import aiomysql

from app.model.schema import WorldcupSourceType
from app.v2.model.dto import WorldcupSessionDTO

STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_ABANDONED = "ABANDONED"


class WorldcupSessionRepository:
    """worldcup_session CRUD 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def abandon_in_progress_sessions(self, user_id: str) -> None:
        """새 월드컵 시작 전, 같은 사용자의 기존 진행 중 세션을 중단 처리합니다."""
        now = datetime.now(timezone.utc)
        sql = (
            "UPDATE worldcup_session "
            "SET status = %s, updated_at = %s "
            "WHERE user_id = %s AND status = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (STATUS_ABANDONED, now, user_id, STATUS_IN_PROGRESS))

    async def create_session(
        self,
        user_id: str,
        source_type: WorldcupSourceType,
        category_id: int | None,
        selected_genres: list[str],
        candidate_pool_size: int,
        round_size: int,
    ) -> WorldcupSessionDTO:
        """월드컵 시작 시 세션 row를 생성합니다."""
        now = datetime.now(timezone.utc)
        selected_genres_json = json.dumps(selected_genres, ensure_ascii=False)

        insert_sql = (
            "INSERT INTO worldcup_session "
            "(user_id, source_type, category_id, selected_genres_json, "
            "candidate_pool_size, round_size, current_round, current_match_order, "
            "status, winner_movie_id, started_at, completed_at, reward_granted, "
            "created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(
                insert_sql,
                (
                    user_id,
                    source_type.value,
                    category_id,
                    selected_genres_json,
                    candidate_pool_size,
                    round_size,
                    round_size,
                    0,
                    STATUS_IN_PROGRESS,
                    None,
                    now,
                    None,
                    False,
                    now,
                    now,
                ),
            )
            session_id = cur.lastrowid

        return WorldcupSessionDTO(
            session_id=session_id,
            user_id=user_id,
            source_type=source_type.value,
            category_id=category_id,
            selected_genres_json=selected_genres_json,
            candidate_pool_size=candidate_pool_size,
            round_size=round_size,
            current_round=round_size,
            current_match_order=0,
            status=STATUS_IN_PROGRESS,
            winner_movie_id=None,
            started_at=now,
            completed_at=None,
            reward_granted=False,
            created_at=now,
            updated_at=now,
        )

    async def advance_round(self, session_id: int, next_round: int) -> None:
        """다음 라운드 진입 시 current_round와 상태를 갱신합니다."""
        now = datetime.now(timezone.utc)
        sql = (
            "UPDATE worldcup_session "
            "SET current_round = %s, current_match_order = %s, status = %s, updated_at = %s "
            "WHERE session_id = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (next_round, 0, STATUS_IN_PROGRESS, now, session_id))

    async def complete_session(self, session_id: int, winner_movie_id: str) -> None:
        """월드컵 종료 시 세션을 완료 처리합니다."""
        now = datetime.now(timezone.utc)
        sql = (
            "UPDATE worldcup_session "
            "SET status = %s, winner_movie_id = %s, completed_at = %s, updated_at = %s "
            "WHERE session_id = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (STATUS_COMPLETED, winner_movie_id, now, now, session_id))
