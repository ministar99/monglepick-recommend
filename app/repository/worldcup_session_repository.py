"""
월드컵 세션 리포지토리

recommend 런타임에서 worldcup_session 테이블을 생성/갱신합니다.
실제 브래킷 상태는 Redis에 저장하지만, 세션 생명주기는 DB에 남깁니다.
"""

import json
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import WorldcupSession
from app.model.schema import WorldcupSourceType

STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_ABANDONED = "ABANDONED"


class WorldcupSessionRepository:
    """worldcup_session CRUD 리포지토리"""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def abandon_in_progress_sessions(self, user_id: str) -> None:
        """새 월드컵 시작 전, 같은 사용자의 기존 진행 중 세션을 중단 처리합니다."""
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(WorldcupSession)
            .where(
                WorldcupSession.user_id == user_id,
                WorldcupSession.status == STATUS_IN_PROGRESS,
            )
            .values(
                status=STATUS_ABANDONED,
                updated_at=now,
            )
        )

    async def create_session(
        self,
        user_id: str,
        source_type: WorldcupSourceType,
        category_id: int | None,
        selected_genres: list[str],
        candidate_pool_size: int,
        round_size: int,
    ) -> WorldcupSession:
        """월드컵 시작 시 세션 row를 생성합니다."""
        now = datetime.now(timezone.utc)
        entity = WorldcupSession(
            user_id=user_id,
            source_type=source_type.value,
            category_id=category_id,
            selected_genres_json=json.dumps(selected_genres, ensure_ascii=False),
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
        self._session.add(entity)
        await self._session.flush()
        return entity

    async def advance_round(self, session_id: int, next_round: int) -> None:
        """다음 라운드 진입 시 current_round와 상태를 갱신합니다."""
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(WorldcupSession)
            .where(WorldcupSession.session_id == session_id)
            .values(
                current_round=next_round,
                current_match_order=0,
                status=STATUS_IN_PROGRESS,
                updated_at=now,
            )
        )

    async def complete_session(self, session_id: int, winner_movie_id: str) -> None:
        """월드컵 종료 시 세션을 완료 처리합니다."""
        now = datetime.now(timezone.utc)
        await self._session.execute(
            update(WorldcupSession)
            .where(WorldcupSession.session_id == session_id)
            .values(
                status=STATUS_COMPLETED,
                winner_movie_id=winner_movie_id,
                completed_at=now,
                updated_at=now,
            )
        )
