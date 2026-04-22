"""
월드컵 매치 리포지토리

recommend 런타임에서 worldcup_match 테이블을 생성/갱신합니다.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import WorldcupMatch as WorldcupMatchEntity


class WorldcupMatchRepository:
    """worldcup_match CRUD 리포지토리"""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create_matches(
        self,
        session_id: int,
        round_number: int,
        candidate_ids: list[str],
    ) -> list[WorldcupMatchEntity]:
        """후보 영화 ID 목록으로 매치 row를 생성합니다."""
        if len(candidate_ids) % 2 != 0:
            raise ValueError(f"매치 생성 후보 수는 짝수여야 합니다: {len(candidate_ids)}")

        now = datetime.now(timezone.utc)
        matches: list[WorldcupMatchEntity] = []
        for index in range(0, len(candidate_ids), 2):
            entity = WorldcupMatchEntity(
                session_id=session_id,
                round_number=round_number,
                match_order=index // 2,
                movie_a_id=candidate_ids[index],
                movie_b_id=candidate_ids[index + 1],
                winner_movie_id=None,
                selected_at=None,
                created_at=now,
                updated_at=now,
            )
            self._session.add(entity)
            matches.append(entity)

        await self._session.flush()
        return matches

    async def get_matches_by_round(
        self,
        session_id: int,
        round_number: int,
    ) -> list[WorldcupMatchEntity]:
        """특정 세션/라운드의 매치를 match_order 순으로 조회합니다."""
        result = await self._session.execute(
            select(WorldcupMatchEntity)
            .where(
                WorldcupMatchEntity.session_id == session_id,
                WorldcupMatchEntity.round_number == round_number,
            )
            .order_by(WorldcupMatchEntity.match_order.asc())
        )
        return list(result.scalars().all())

    async def select_round_winners(
        self,
        session_id: int,
        round_number: int,
        selected_ids: list[str],
    ) -> list[WorldcupMatchEntity]:
        """라운드별 승자 선택 결과를 DB 매치 row에 반영합니다."""
        matches = await self.get_matches_by_round(session_id, round_number)
        if len(matches) != len(selected_ids):
            raise ValueError(
                "제출된 승자 수와 저장된 매치 수가 일치하지 않습니다: "
                f"matches={len(matches)}, selections={len(selected_ids)}"
            )

        now = datetime.now(timezone.utc)
        for match, winner_movie_id in zip(matches, selected_ids):
            if winner_movie_id not in (match.movie_a_id, match.movie_b_id):
                raise ValueError(
                    "승자 영화 ID가 해당 매치의 대결 영화가 아닙니다: "
                    f"match_id={match.match_id}, winner={winner_movie_id}"
                )
            match.winner_movie_id = winner_movie_id
            match.selected_at = now
            match.updated_at = now
            self._session.add(match)

        await self._session.flush()
        return matches
