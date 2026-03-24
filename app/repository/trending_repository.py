"""
인기 검색어 리포지토리

MySQL trending_keywords 테이블에 대한 CRUD를 담당합니다.
Redis Sorted Set과 함께 사용하며, MySQL은 영속적인 백업/통계 분석용입니다.

실시간 순위 관리는 Redis에서 수행하고,
주기적으로 MySQL에 동기화합니다.
"""

from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import TrendingKeyword


class TrendingRepository:
    """인기 검색어 MySQL 리포지토리"""

    def __init__(self, session: AsyncSession):
        """
        Args:
            session: SQLAlchemy 비동기 세션
        """
        self._session = session

    async def increment(self, keyword: str) -> TrendingKeyword:
        """
        검색어의 누적 검색 횟수를 1 증가시킵니다.

        해당 키워드가 없으면 새로 생성하고(count=1),
        이미 존재하면 search_count를 +1 합니다.

        Args:
            keyword: 검색 키워드

        Returns:
            갱신된 TrendingKeyword 엔티티
        """
        keyword_cleaned = keyword.strip()

        # 기존 키워드 조회
        result = await self._session.execute(
            select(TrendingKeyword).where(TrendingKeyword.keyword == keyword_cleaned)
        )
        existing = result.scalar_one_or_none()

        if existing:
            # 기존 키워드: 검색 횟수 +1, 마지막 검색 시각 갱신
            existing.search_count += 1
            existing.last_searched_at = datetime.now(timezone.utc)
            self._session.add(existing)
            await self._session.flush()
            return existing
        else:
            # 새 키워드: 생성
            new_keyword = TrendingKeyword(
                keyword=keyword_cleaned,
                search_count=1,
                last_searched_at=datetime.now(timezone.utc),
            )
            self._session.add(new_keyword)
            await self._session.flush()
            return new_keyword

    async def get_top_keywords(self, limit: int = 10) -> list[TrendingKeyword]:
        """
        검색 횟수 기준 상위 인기 검색어를 반환합니다.

        Args:
            limit: 반환할 최대 건수 (기본 10)

        Returns:
            인기 검색어 목록 (검색 횟수 내림차순)
        """
        result = await self._session.execute(
            select(TrendingKeyword)
            .order_by(TrendingKeyword.search_count.desc())
            .limit(limit)
        )
        return list(result.scalars().all())
