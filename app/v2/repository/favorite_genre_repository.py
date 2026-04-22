"""
선호 장르 리포지토리 (v2 Raw SQL)

fav_genre / genre_master 테이블에 대한 읽기/쓰기를 담당합니다.
마이페이지 선호 설정 탭에서 사용할 장르 옵션과 사용자 저장값을 관리합니다.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiomysql


class FavoriteGenreRepository:
    """fav_genre CRUD 리포지토리."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def list_available_genres(self, excluded_names: list[str]) -> list[dict]:
        """genre_master에서 노출 가능한 장르 목록을 반환합니다."""
        params: list[object] = []
        sql = (
            "SELECT genre_id, genre_code, genre_name, contents_count "
            "FROM genre_master "
        )

        if excluded_names:
            placeholders = ", ".join(["%s"] * len(excluded_names))
            sql += f"WHERE genre_name NOT IN ({placeholders}) "
            params.extend(excluded_names)

        sql += "ORDER BY contents_count DESC, genre_name ASC"

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, tuple(params))
            return await cur.fetchall()

    async def list_selected_by_user(self, user_id: str) -> list[dict]:
        """사용자가 저장한 선호 장르 목록을 priority 순으로 반환합니다."""
        sql = (
            "SELECT "
            "  fg.fav_genre_id, "
            "  fg.user_id, "
            "  fg.genre_id, "
            "  fg.priority, "
            "  fg.created_at, "
            "  gm.genre_code, "
            "  gm.genre_name, "
            "  gm.contents_count "
            "FROM fav_genre fg "
            "JOIN genre_master gm ON gm.genre_id = fg.genre_id "
            "WHERE fg.user_id = %s "
            "ORDER BY COALESCE(fg.priority, 999999) ASC, fg.fav_genre_id ASC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            return await cur.fetchall()

    async def replace_all(self, user_id: str, genre_ids: list[int]) -> None:
        """
        사용자의 선호 장르 목록을 전달된 순서 그대로 전체 교체합니다.

        - 목록에 없는 기존 장르는 삭제
        - 목록에 있는 장르는 priority 갱신
        - 새 장르는 INSERT
        """
        now = datetime.now(timezone.utc)

        if not genre_ids:
            async with self._conn.cursor() as cur:
                await cur.execute("DELETE FROM fav_genre WHERE user_id = %s", (user_id,))
            return

        placeholders = ", ".join(["%s"] * len(genre_ids))
        delete_sql = (
            "DELETE FROM fav_genre "
            f"WHERE user_id = %s AND genre_id NOT IN ({placeholders})"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(delete_sql, (user_id, *genre_ids))

        values_sql = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(genre_ids))
        insert_sql = (
            "INSERT INTO fav_genre "
            "(user_id, genre_id, priority, created_at, updated_at, created_by, updated_by) "
            f"VALUES {values_sql} "
            "ON DUPLICATE KEY UPDATE "
            "priority = VALUES(priority), "
            "updated_at = VALUES(updated_at), "
            "updated_by = VALUES(updated_by)"
        )

        params: list[object] = []
        for priority, genre_id in enumerate(genre_ids, start=1):
            params.extend([user_id, genre_id, priority, now, now, user_id, user_id])

        async with self._conn.cursor() as cur:
            await cur.execute(insert_sql, tuple(params))
