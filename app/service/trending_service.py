"""
인기 검색어 서비스

REQ_032: 인기/실시간 검색어 TOP 10

Redis Sorted Set을 사용하여 실시간 인기 검색어를 관리합니다.
각 검색 시 해당 키워드의 score를 +1 하고,
ZREVRANGE로 상위 N개를 조회합니다.

Redis 키:
- trending:keywords → Sorted Set
  - member: 검색 키워드 (문자열)
  - score: 누적 검색 횟수 (float)

MySQL trending_keywords 테이블은 영속적 백업용으로,
Redis 장애 시 폴백 데이터 소스로 사용합니다.
"""

import logging

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.schema import TrendingKeywordItem, TrendingResponse
from app.repository.trending_repository import TrendingRepository

logger = logging.getLogger(__name__)

# Redis Sorted Set 키 이름
TRENDING_REDIS_KEY = "trending:keywords"


class TrendingService:
    """인기 검색어 집계 서비스"""

    def __init__(self, session: AsyncSession, redis_client: aioredis.Redis):
        """
        Args:
            session: SQLAlchemy 비동기 세션
            redis_client: Redis 비동기 클라이언트
        """
        self._session = session
        self._redis = redis_client
        self._settings = get_settings()
        self._trending_repo = TrendingRepository(session)

    async def get_trending(self) -> TrendingResponse:
        """
        인기 검색어 TOP K를 반환합니다.

        1차: Redis Sorted Set에서 score 내림차순으로 조회
        2차 (Redis 장애 시): MySQL trending_keywords 테이블에서 조회

        Returns:
            TrendingResponse: 인기 검색어 목록 (순위, 키워드, 검색 횟수)
        """
        top_k = self._settings.TRENDING_TOP_K

        # ─────────────────────────────────────
        # 1차: Redis Sorted Set 조회
        # ─────────────────────────────────────
        try:
            # ZREVRANGE: score 내림차순으로 상위 K개 조회 (withscores=True)
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

        # ─────────────────────────────────────
        # 2차: MySQL 폴백 (Redis 장애 또는 데이터 없음)
        # ─────────────────────────────────────
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

        Redis Sorted Set의 score를 +1 하고,
        MySQL에도 동기화합니다.

        이 메서드는 SearchService에서 검색 시 자동 호출되므로,
        보통 직접 호출할 필요는 없습니다.

        Args:
            keyword: 검색 키워드
        """
        keyword_cleaned = keyword.strip()
        if not keyword_cleaned:
            return

        # Redis Sorted Set score +1
        try:
            await self._redis.zincrby(TRENDING_REDIS_KEY, 1, keyword_cleaned)
        except Exception as e:
            logger.warning(f"Redis 인기 검색어 기록 실패: {e}")

        # MySQL 백업
        try:
            await self._trending_repo.increment(keyword_cleaned)
        except Exception as e:
            logger.warning(f"MySQL 인기 검색어 기록 실패: {e}")
