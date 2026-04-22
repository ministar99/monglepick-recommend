"""
월드컵 매치 리포지토리 (v2 Raw SQL)

recommend 런타임에서 worldcup_match 테이블을 생성/갱신합니다.
"""

from datetime import datetime, timezone

import aiomysql


class WorldcupMatchRepository:
    """worldcup_match CRUD 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def create_matches(
        self,
        session_id: int,
        round_number: int,
        candidate_ids: list[str],
    ) -> list[dict]:
        """후보 영화 ID 목록으로 매치 row를 생성합니다."""
        if len(candidate_ids) % 2 != 0:
            raise ValueError(f"매치 생성 후보 수는 짝수여야 합니다: {len(candidate_ids)}")

        now = datetime.now(timezone.utc)
        insert_sql = (
            "INSERT INTO worldcup_match "
            "(session_id, round_number, match_order, movie_a_id, movie_b_id, "
            "winner_movie_id, selected_at, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        created_rows: list[dict] = []
        async with self._conn.cursor() as cur:
            for index in range(0, len(candidate_ids), 2):
                params = (
                    session_id,
                    round_number,
                    index // 2,
                    candidate_ids[index],
                    candidate_ids[index + 1],
                    None,
                    None,
                    now,
                    now,
                )
                await cur.execute(insert_sql, params)
                created_rows.append(
                    {
                        "match_id": cur.lastrowid,
                        "session_id": session_id,
                        "round_number": round_number,
                        "match_order": index // 2,
                        "movie_a_id": candidate_ids[index],
                        "movie_b_id": candidate_ids[index + 1],
                        "winner_movie_id": None,
                        "selected_at": None,
                    }
                )
        return created_rows

    async def get_matches_by_round(
        self,
        session_id: int,
        round_number: int,
    ) -> list[dict]:
        """특정 세션/라운드의 매치를 match_order 순으로 조회합니다."""
        sql = (
            "SELECT match_id, session_id, round_number, match_order, movie_a_id, movie_b_id, "
            "winner_movie_id, selected_at "
            "FROM worldcup_match "
            "WHERE session_id = %s AND round_number = %s "
            "ORDER BY match_order ASC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (session_id, round_number))
            return list(await cur.fetchall())

    async def select_round_winners(
        self,
        session_id: int,
        round_number: int,
        selected_ids: list[str],
    ) -> list[dict]:
        """라운드별 승자 선택 결과를 DB 매치 row에 반영합니다."""
        matches = await self.get_matches_by_round(session_id, round_number)
        if len(matches) != len(selected_ids):
            raise ValueError(
                "제출된 승자 수와 저장된 매치 수가 일치하지 않습니다: "
                f"matches={len(matches)}, selections={len(selected_ids)}"
            )

        now = datetime.now(timezone.utc)
        update_sql = (
            "UPDATE worldcup_match "
            "SET winner_movie_id = %s, selected_at = %s, updated_at = %s "
            "WHERE match_id = %s"
        )
        async with self._conn.cursor() as cur:
            for match, winner_movie_id in zip(matches, selected_ids):
                if winner_movie_id not in (match["movie_a_id"], match["movie_b_id"]):
                    raise ValueError(
                        "승자 영화 ID가 해당 매치의 대결 영화가 아닙니다: "
                        f"match_id={match['match_id']}, winner={winner_movie_id}"
                    )
                await cur.execute(
                    update_sql,
                    (winner_movie_id, now, now, match["match_id"]),
                )
                match["winner_movie_id"] = winner_movie_id
                match["selected_at"] = now

        return matches
