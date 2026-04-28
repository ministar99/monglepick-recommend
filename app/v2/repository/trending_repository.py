"""
인기 검색어 리포지토리 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 TrendingRepository를 Raw SQL로 재구현합니다.
MySQL trending_keywords 테이블에 대한 CRUD를 담당합니다.

Redis Sorted Set과 함께 사용하며, MySQL은 영속적인 백업/통계 분석용입니다.
"""

import logging
from datetime import datetime, timezone

import aiomysql

from app.v2.model.dto import TrendingKeywordDTO

logger = logging.getLogger(__name__)


class TrendingRepository:
    """인기 검색어 MySQL 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        """
        Args:
            conn: aiomysql 비동기 커넥션
        """
        self._conn = conn

    async def increment(self, keyword: str) -> TrendingKeywordDTO:
        """
        검색어의 누적 검색 횟수를 1 증가시킵니다.

        해당 키워드가 없으면 새로 생성하고(count=1),
        이미 존재하면 search_count를 +1 합니다.

        Args:
            keyword: 검색 키워드

        Returns:
            갱신된 TrendingKeywordDTO
        """
        keyword_cleaned = keyword.strip()
        now = datetime.now(timezone.utc)
        upsert_sql = (
            "INSERT INTO trending_keywords (keyword, search_count, last_searched_at) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "search_count = search_count + 1, "
            "last_searched_at = VALUES(last_searched_at)"
        )
        select_sql = "SELECT * FROM trending_keywords WHERE keyword = %s"

        async with self._conn.cursor() as cur:
            await cur.execute(upsert_sql, (keyword_cleaned, 1, now))

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(select_sql, (keyword_cleaned,))
            row = await cur.fetchone()

        return TrendingKeywordDTO(**row)

    async def get_top_keywords(self, limit: int = 10) -> list[TrendingKeywordDTO]:
        """
        검색 횟수 기준 상위 인기 검색어를 반환합니다.

        Args:
            limit: 반환할 최대 건수 (기본 10)

        Returns:
            인기 검색어 DTO 목록 (검색 횟수 내림차순)
        """
        sql = (
            "SELECT * FROM trending_keywords "
            "ORDER BY search_count DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()

        return [TrendingKeywordDTO(**row) for row in rows]

    async def get_recent_top_keywords(
        self,
        since: datetime,
        limit: int = 20,
    ) -> list[TrendingKeywordDTO]:
        """
        기준 시각 이후에 검색된 인기 키워드를 조회합니다.

        Args:
            since: 포함 시작 시각 (UTC)
            limit: 반환할 최대 건수

        Returns:
            최근 활성 키워드 DTO 목록
        """
        sql = (
            "SELECT * FROM trending_keywords "
            "WHERE last_searched_at >= %s "
            "ORDER BY search_count DESC, last_searched_at DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (since, limit))
            rows = await cur.fetchall()

        return [TrendingKeywordDTO(**row) for row in rows]
