"""
검색 초기 화면 개인화 추천용 추가 사용자 신호 리포지토리 (v2 Raw SQL).

SearchPage 상단 예상 픽은 recommend 서비스 안에서 끝내야 하므로,
기존 fav/review/wishlist 외에 user_behavior_profile, user_implicit_rating,
recommendation_impact 같은 보조 신호도 여기서 직접 읽어옵니다.
"""

from __future__ import annotations

import json
from typing import Any

import aiomysql


class PersonalizedSearchRepository:
    """개인화 추천용 사용자 보조 신호 조회 리포지토리."""

    _ALLOWED_TABLES: frozenset[str] = frozenset(
        {
            "recommendation_impact",
            "user_behavior_profile",
            "user_implicit_rating",
            "user_watch_history",
        }
    )

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._columns_cache: dict[str, set[str]] = {}
        self._table_exists_cache: dict[str, bool] = {}

    async def _table_exists(self, table_name: str) -> bool:
        """지정한 테이블 존재 여부를 캐시합니다."""
        if table_name not in self._ALLOWED_TABLES:
            raise ValueError(f"허용되지 않은 테이블: {table_name}")

        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SHOW TABLES LIKE %s", (table_name,))
            row = await cur.fetchone()

        exists = row is not None
        self._table_exists_cache[table_name] = exists
        return exists

    async def _get_columns(self, table_name: str) -> set[str]:
        """테이블 컬럼 목록을 캐시합니다."""
        if table_name not in self._ALLOWED_TABLES:
            raise ValueError(f"허용되지 않은 테이블: {table_name}")

        if table_name in self._columns_cache:
            return self._columns_cache[table_name]

        if not await self._table_exists(table_name):
            self._columns_cache[table_name] = set()
            return set()

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SHOW COLUMNS FROM {table_name}")
            rows = await cur.fetchall()

        columns = {row["Field"] for row in rows or []}
        self._columns_cache[table_name] = columns
        return columns

    async def list_dismissed_movie_ids(self, user_id: str, limit: int = 200) -> list[str]:
        """관심 없음으로 표시된 영화 ID 목록을 최신순으로 반환합니다."""
        columns = await self._get_columns("recommendation_impact")
        if not columns or "dismissed" not in columns:
            return []

        order_column = "updated_at" if "updated_at" in columns else "created_at"
        sql = (
            "SELECT movie_id "
            "FROM recommendation_impact "
            "WHERE user_id = %s "
            "  AND COALESCE(dismissed, 0) <> 0 "
            f"ORDER BY {order_column} DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, limit))
            rows = await cur.fetchall()

        return [
            str(row["movie_id"]).strip()
            for row in rows or []
            if row.get("movie_id")
        ]

    async def list_watched_movie_ids(self, user_id: str, limit: int = 80) -> list[str]:
        """
        사용자가 이미 본 영화 ID를 반환합니다.

        운영 스키마의 user_watch_history가 있으면 우선 사용하고,
        없으면 recommendation_impact.watched/rated 신호로 대체합니다.
        """
        watch_columns = await self._get_columns("user_watch_history")
        if watch_columns:
            order_column = "watched_at" if "watched_at" in watch_columns else "created_at"
            sql = (
                "SELECT movie_id "
                "FROM user_watch_history "
                "WHERE user_id = %s "
                f"ORDER BY {order_column} DESC "
                "LIMIT %s"
            )
            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, (user_id, limit))
                rows = await cur.fetchall()

            return [
                str(row["movie_id"]).strip()
                for row in rows or []
                if row.get("movie_id")
            ]

        impact_columns = await self._get_columns("recommendation_impact")
        if not impact_columns:
            return []

        watched_clauses: list[str] = []
        if "watched" in impact_columns:
            watched_clauses.append("COALESCE(watched, 0) <> 0")
        if "rated" in impact_columns:
            watched_clauses.append("COALESCE(rated, 0) <> 0")
        if not watched_clauses:
            return []

        order_column = "updated_at" if "updated_at" in impact_columns else "created_at"
        sql = (
            "SELECT movie_id "
            "FROM recommendation_impact "
            "WHERE user_id = %s "
            f"  AND ({' OR '.join(watched_clauses)}) "
            f"ORDER BY {order_column} DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, limit))
            rows = await cur.fetchall()

        return [
            str(row["movie_id"]).strip()
            for row in rows or []
            if row.get("movie_id")
        ]

    async def list_top_implicit_movie_ids(self, user_id: str, limit: int = 6) -> list[str]:
        """암묵적 반응 점수가 높은 영화 ID를 상위 순으로 반환합니다."""
        columns = await self._get_columns("user_implicit_rating")
        if not columns:
            return []

        order_parts = ["implicit_score DESC"]
        if "last_action_at" in columns:
            order_parts.append("last_action_at DESC")
        if "updated_at" in columns:
            order_parts.append("updated_at DESC")

        sql = (
            "SELECT movie_id "
            "FROM user_implicit_rating "
            "WHERE user_id = %s "
            f"ORDER BY {', '.join(order_parts)} "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, limit))
            rows = await cur.fetchall()

        return [
            str(row["movie_id"]).strip()
            for row in rows or []
            if row.get("movie_id")
        ]

    async def get_behavior_profile(self, user_id: str) -> dict[str, Any]:
        """행동 프로필을 읽어 JSON 필드를 파싱한 dict로 반환합니다."""
        columns = await self._get_columns("user_behavior_profile")
        if not columns:
            return {}

        select_columns = [
            column
            for column in (
                "genre_affinity",
                "mood_affinity",
                "director_affinity",
                "taste_consistency",
                "recommendation_acceptance_rate",
                "avg_exploration_depth",
                "activity_level",
            )
            if column in columns
        ]
        if not select_columns:
            return {}

        sql = (
            f"SELECT {', '.join(select_columns)} "
            "FROM user_behavior_profile "
            "WHERE user_id = %s "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            row = await cur.fetchone()

        if not row:
            return {}

        profile = dict(row)
        for json_key in ("genre_affinity", "mood_affinity", "director_affinity"):
            value = profile.get(json_key)
            profile[json_key] = self._parse_json_object(value)

        return profile

    @staticmethod
    def _parse_json_object(value: Any) -> dict[str, Any]:
        """JSON 문자열 또는 dict를 dict로 정규화합니다."""
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}
