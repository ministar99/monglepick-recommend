"""
인기 검색어 리포지토리

MySQL trending_keywords 테이블에 대한 CRUD를 담당합니다.
Redis Sorted Set과 함께 사용하며, MySQL은 영속적인 백업/통계 분석용입니다.

실시간 순위 관리는 Redis에서 수행하고,
주기적으로 MySQL에 동기화합니다.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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
        now = datetime.now(timezone.utc)
        dialect_name = self._session.get_bind().dialect.name

        if dialect_name == "mysql":
            stmt = mysql_insert(TrendingKeyword).values(
                keyword=keyword_cleaned,
                search_count=1,
                last_searched_at=now,
            )
            stmt = stmt.on_duplicate_key_update(
                search_count=TrendingKeyword.search_count + 1,
                last_searched_at=stmt.inserted.last_searched_at,
            )
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(TrendingKeyword).values(
                keyword=keyword_cleaned,
                search_count=1,
                last_searched_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[TrendingKeyword.keyword],
                set_={
                    "search_count": TrendingKeyword.search_count + 1,
                    "last_searched_at": stmt.excluded.last_searched_at,
                },
            )
        else:
            return await self._increment_legacy(keyword_cleaned, now)

        # 인기 검색어 백업 실패가 요청 전체 트랜잭션을 깨뜨리지 않도록 savepoint로 격리합니다.
        async with self._session.begin_nested():
            await self._session.execute(stmt)

        # 같은 세션에서 최신 상태를 다시 읽어 반환합니다.
        result = await self._session.execute(
            select(TrendingKeyword).where(TrendingKeyword.keyword == keyword_cleaned)
        )
        return result.scalar_one()

    async def _increment_legacy(self, keyword: str, now: datetime) -> TrendingKeyword:
        """
        sqlite/mysql 외 테스트성 dialect를 위한 폴백 구현입니다.

        운영 경로는 dialect별 upsert를 사용하고, 이 경로는 호환성 유지 목적입니다.
        """
        result = await self._session.execute(
            select(TrendingKeyword).where(TrendingKeyword.keyword == keyword)
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.search_count += 1
            existing.last_searched_at = now
            self._session.add(existing)
            await self._session.flush()
            return existing

        new_keyword = TrendingKeyword(
            keyword=keyword,
            search_count=1,
            last_searched_at=now,
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
