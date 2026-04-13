"""
위시리스트 리포지토리 (v2 Raw SQL)

Recommend(FastAPI)에서 user_wishlist 테이블을 직접 조회/조작한다.
마이페이지와 영화 상세의 위시리스트 UX를 recommend 서비스만으로 처리하기 위한
읽기/쓰기 SQL을 한 곳에 모아 둔다.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiomysql


class WishlistRepository:
    """위시리스트 MySQL 리포지토리."""

    # SQL Injection 방지: _get_columns() 에 전달 가능한 테이블명 화이트리스트.
    # 이 집합에 없는 테이블명이 인자로 들어오면 즉시 ValueError 를 발생시킨다.
    _ALLOWED_TABLES: frozenset[str] = frozenset({"user_wishlist"})

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._columns_cache: dict[str, set[str]] = {}

    async def _get_columns(self, table_name: str) -> set[str]:
        """
        테이블 컬럼 목록을 캐시한다.

        로컬 DB가 구버전 스키마(id/content)일 수도 있고,
        최신 운영 스키마(wishlist_id/contents)일 수도 있어 런타임에 실제 컬럼을 확인한다.

        SQL Injection 방지를 위해 table_name 을 _ALLOWED_TABLES 로 검증한다.
        허용 목록에 없는 테이블명이 전달되면 ValueError 를 발생시킨다.
        """
        # 허용 목록 검증 — SHOW COLUMNS 는 파라미터 바인딩을 지원하지 않아
        # f-string 으로 테이블명을 삽입하기 전 반드시 allowlist 를 통과해야 한다.
        if table_name not in self._ALLOWED_TABLES:
            raise ValueError(f"허용되지 않은 테이블: {table_name}")

        if table_name in self._columns_cache:
            return self._columns_cache[table_name]

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SHOW COLUMNS FROM {table_name}")
            rows = await cur.fetchall()

        columns = {row["Field"] for row in rows or []}
        self._columns_cache[table_name] = columns
        return columns

    async def list_by_user(
        self,
        user_id: str,
        offset: int,
        limit: int,
    ) -> list[dict]:
        """
        사용자의 위시리스트와 영화 카드용 메타 정보를 함께 조회한다.

        movies 테이블과 JOIN하여 마이페이지에서 바로 렌더링할 수 있는 정보를 만든다.
        """
        columns = await self._get_columns("user_wishlist")
        wishlist_id_column = "wishlist_id" if "wishlist_id" in columns else "id"

        sql = (
            "SELECT "
            f"  uw.{wishlist_id_column} AS wishlist_id, "
            "  uw.movie_id AS wishlist_movie_id, "
            "  uw.created_at AS wishlist_created_at, "
            "  m.movie_id, m.title, m.title_en, m.poster_path, m.release_year, "
            "  m.rating, m.vote_count, m.genres, m.trailer_url, m.overview "
            "FROM user_wishlist uw "
            "JOIN movies m ON m.movie_id = uw.movie_id "
            "WHERE uw.user_id = %s "
            "ORDER BY uw.created_at DESC "
            "LIMIT %s OFFSET %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, limit, offset))
            return list(await cur.fetchall() or [])

    async def count_by_user(self, user_id: str) -> int:
        """사용자의 전체 위시리스트 개수를 반환한다."""
        sql = "SELECT COUNT(*) AS cnt FROM user_wishlist WHERE user_id = %s"
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            row = await cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0

    async def exists(self, user_id: str, movie_id: str) -> bool:
        """현재 영화가 위시리스트에 이미 담겨 있는지 확인한다."""
        sql = (
            "SELECT 1 "
            "FROM user_wishlist "
            "WHERE user_id = %s AND movie_id = %s "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, movie_id))
            row = await cur.fetchone()
        return row is not None

    async def add(self, user_id: str, movie_id: str) -> None:
        """
        위시리스트에 영화를 추가한다.

        BaseAuditEntity 컬럼이 있는 최신 스키마와
        created_at만 있는 구버전 스키마를 모두 지원한다.
        """
        now = datetime.now(timezone.utc)
        columns = await self._get_columns("user_wishlist")
        insert_columns = ["user_id", "movie_id"]
        values: list[object] = [user_id, movie_id]

        if "created_at" in columns:
            insert_columns.append("created_at")
            values.append(now)
        if "updated_at" in columns:
            insert_columns.append("updated_at")
            values.append(now)
        if "created_by" in columns:
            insert_columns.append("created_by")
            values.append(user_id)
        if "updated_by" in columns:
            insert_columns.append("updated_by")
            values.append(user_id)

        placeholders = ", ".join(["%s"] * len(insert_columns))
        sql = (
            "INSERT INTO user_wishlist "
            f"({', '.join(insert_columns)}) "
            f"VALUES ({placeholders})"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, tuple(values))

    async def remove(self, user_id: str, movie_id: str) -> bool:
        """위시리스트에서 영화를 제거한다. 삭제 여부를 반환한다."""
        sql = (
            "DELETE FROM user_wishlist "
            "WHERE user_id = %s AND movie_id = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (user_id, movie_id))
            return cur.rowcount > 0
