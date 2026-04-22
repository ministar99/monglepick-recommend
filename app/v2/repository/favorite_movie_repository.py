"""
최애 영화 리포지토리 (v2 Raw SQL)

fav_movie 테이블에 대한 읽기/쓰기를 담당합니다.
사용자별 최대 9편의 최애 영화와 priority 순서를 관리합니다.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiomysql


class FavoriteMovieRepository:
    """fav_movie CRUD 리포지토리."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def list_by_user(self, user_id: str) -> list[dict]:
        """사용자의 최애 영화 목록을 priority 순으로 반환합니다."""
        sql = (
            "SELECT fav_movie_id, user_id, movie_id, priority, created_at, updated_at "
            "FROM fav_movie "
            "WHERE user_id = %s "
            "ORDER BY COALESCE(priority, 999999) ASC, fav_movie_id ASC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            return await cur.fetchall()

    async def replace_all(self, user_id: str, movie_ids: list[str]) -> None:
        """
        사용자의 최애 영화 목록을 전달된 순서 그대로 전체 교체합니다.

        - 목록에 없는 기존 영화는 삭제
        - 목록에 있는 영화는 priority 갱신
        - 새 영화는 INSERT
        """
        now = datetime.now(timezone.utc)

        if not movie_ids:
            async with self._conn.cursor() as cur:
                await cur.execute("DELETE FROM fav_movie WHERE user_id = %s", (user_id,))
            return

        placeholders = ", ".join(["%s"] * len(movie_ids))
        delete_sql = (
            "DELETE FROM fav_movie "
            f"WHERE user_id = %s AND movie_id NOT IN ({placeholders})"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(delete_sql, (user_id, *movie_ids))

        values_sql = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(movie_ids))
        insert_sql = (
            "INSERT INTO fav_movie "
            "(user_id, movie_id, priority, created_at, updated_at, created_by, updated_by) "
            f"VALUES {values_sql} "
            "ON DUPLICATE KEY UPDATE "
            "priority = VALUES(priority), "
            "updated_at = VALUES(updated_at), "
            "updated_by = VALUES(updated_by)"
        )

        params: list[object] = []
        for priority, movie_id in enumerate(movie_ids, start=1):
            params.extend([user_id, movie_id, priority, now, now, user_id, user_id])

        async with self._conn.cursor() as cur:
            await cur.execute(insert_sql, tuple(params))
