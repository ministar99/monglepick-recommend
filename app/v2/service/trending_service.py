"""
인기 검색어 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 TrendingService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직(Redis 우선 조회, MySQL 폴백)은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import logging
from datetime import datetime, timedelta, timezone

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    AdminPopularKeywordItem,
    AdminPopularKeywordsResponse,
    TrendingKeywordItem,
    TrendingResponse,
)
from app.v2.repository.search_history_repository import SearchHistoryRepository
from app.v2.repository.trending_repository import TrendingRepository

logger = logging.getLogger(__name__)

# Redis Sorted Set 키 이름
TRENDING_REDIS_KEY = "trending:keywords"


class TrendingService:
    """인기 검색어 집계 서비스 (v2 Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection, redis_client: aioredis.Redis):
        """
        Args:
            conn: aiomysql 비동기 커넥션
            redis_client: Redis 비동기 클라이언트
        """
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._trending_repo = TrendingRepository(conn)
        self._history_repo = SearchHistoryRepository(conn)

    async def get_trending(self) -> TrendingResponse:
        """
        인기 검색어 TOP K를 반환합니다.

        1차: Redis Sorted Set에서 score 내림차순으로 조회
        2차 (Redis 장애 시): MySQL trending_keywords 테이블에서 조회
        """
        top_k = self._settings.TRENDING_TOP_K

        # 1차: Redis Sorted Set 조회
        try:
            results = await self._redis.zrevrange(
                TRENDING_REDIS_KEY,
                0,
                top_k - 1,
                withscores=True,
            )

            if results:
                items = []
                for rank, (keyword, score) in enumerate(results, start=1):
                    items.append(
                        TrendingKeywordItem(
                            rank=rank,
                            keyword=keyword,
                            search_count=int(score),
                        )
                    )
                return TrendingResponse(keywords=items)

        except Exception as e:
            logger.warning(f"Redis 인기 검색어 조회 실패, MySQL 폴백: {e}")

        # 2차: MySQL 폴백
        db_keywords = await self._trending_repo.get_top_keywords(limit=top_k)
        items = [
            TrendingKeywordItem(
                rank=rank,
                keyword=kw.keyword,
                search_count=kw.search_count,
            )
            for rank, kw in enumerate(db_keywords, start=1)
        ]
        return TrendingResponse(keywords=items)

    async def record_search(self, keyword: str) -> None:
        """
        검색어를 인기 검색어에 기록합니다.

        Redis Sorted Set의 score를 +1 하고, MySQL에도 동기화합니다.
        """
        keyword_cleaned = keyword.strip()
        if not keyword_cleaned:
            return

        try:
            await self._redis.zincrby(TRENDING_REDIS_KEY, 1, keyword_cleaned)
        except Exception as e:
            logger.warning(f"Redis 인기 검색어 기록 실패: {e}")

        try:
            await self._trending_repo.increment(keyword_cleaned)
        except Exception as e:
            logger.warning(f"MySQL 인기 검색어 기록 실패: {e}")

    async def get_admin_popular_keywords(
        self,
        period: str = "7d",
        limit: int = 20,
    ) -> AdminPopularKeywordsResponse:
        """
        관리자 검색 분석 탭용 인기 검색어 목록을 반환합니다.

        - trending_keywords: 키워드 후보, 검색 수, 기본 정렬 기준
        - search_history: 기간 내 클릭 전환율 계산
        """
        days = self._parse_period_days(period)
        since = datetime.now(timezone.utc) - timedelta(days=days)
        candidate_limit = max(limit * 5, 50)

        trending_rows = await self._trending_repo.get_recent_top_keywords(
            since=since,
            limit=candidate_limit,
        )
        if not trending_rows:
            return AdminPopularKeywordsResponse(keywords=[])

        keyword_order = [row.keyword for row in trending_rows]
        history_stats = await self._history_repo.get_keyword_stats_since(since, keyword_order)

        items: list[AdminPopularKeywordItem] = []
        for row in trending_rows:
            stats = history_stats.get(row.keyword, {})
            period_search_count = int(stats.get("search_count") or 0)
            click_count = int(stats.get("click_count") or 0)
            conversion_rate = (
                round(click_count / period_search_count, 4)
                if period_search_count > 0
                else 0.0
            )
            items.append(
                AdminPopularKeywordItem(
                    keyword=row.keyword,
                    search_count=row.search_count,
                    conversion_rate=conversion_rate,
                )
            )

        return AdminPopularKeywordsResponse(
            keywords=items[:limit]
        )

    @staticmethod
    def _parse_period_days(period: str | None) -> int:
        """관리자 기간 문자열(1d/7d/30d)을 일 수로 변환합니다."""
        if not period or not period.strip():
            return 7

        normalized = period.strip().lower()
        try:
            days = int(normalized.replace("d", ""))
            return max(days, 1)
        except ValueError:
            logger.warning("관리자 인기 검색어 period 파싱 실패: %s", period)
            return 7
