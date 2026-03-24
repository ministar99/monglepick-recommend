"""
검색 이력 리포지토리

사용자별 최근 검색어의 CRUD를 담당합니다.
동일 키워드 재검색 시 타임스탬프만 갱신하며,
최대 보관 건수(20건)를 초과하면 가장 오래된 항목을 삭제합니다.

search_history 테이블은 이 서비스가 소유합니다.
"""

from datetime import datetime, timezone

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.entity import SearchHistory


class SearchHistoryRepository:
    """검색 이력 CRUD 리포지토리"""

    def __init__(self, session: AsyncSession):
        """
        Args:
            session: SQLAlchemy 비동기 세션
        """
        self._session = session
        self._settings = get_settings()

    async def add_search(self, user_id: str, keyword: str) -> SearchHistory:
        """
        검색 이력을 추가하거나 기존 키워드의 타임스탬프를 갱신합니다.

        동일 키워드가 이미 존재하면 searched_at만 현재 시각으로 갱신하고,
        새 키워드이면 INSERT합니다. 최대 보관 건수를 초과하면 오래된 것을 삭제합니다.

        Args:
            user_id: 사용자 ID
            keyword: 검색 키워드 (공백 제거 후 저장)

        Returns:
            저장/갱신된 SearchHistory 엔티티
        """
        keyword_cleaned = keyword.strip()

        # 기존 동일 키워드 검색
        existing = await self._session.execute(
            select(SearchHistory).where(
                SearchHistory.user_id == user_id,
                SearchHistory.keyword == keyword_cleaned,
            )
        )
        existing_record = existing.scalar_one_or_none()

        if existing_record:
            # 기존 키워드: 타임스탬프만 갱신
            existing_record.searched_at = datetime.now(timezone.utc)
            self._session.add(existing_record)
            await self._session.flush()
            return existing_record
        else:
            # 새 키워드: INSERT
            new_record = SearchHistory(
                user_id=user_id,
                keyword=keyword_cleaned,
                searched_at=datetime.now(timezone.utc),
            )
            self._session.add(new_record)
            await self._session.flush()

            # 최대 보관 건수 초과 시 오래된 항목 삭제
            await self._trim_old_records(user_id)

            return new_record

    async def get_recent(
        self, user_id: str, limit: int | None = None
    ) -> list[SearchHistory]:
        """
        사용자의 최근 검색어를 최신순으로 반환합니다.

        Args:
            user_id: 사용자 ID
            limit: 최대 반환 건수 (None이면 설정값 사용)

        Returns:
            최근 검색 이력 목록 (최신순 정렬)
        """
        max_count = limit or self._settings.RECENT_SEARCH_MAX
        result = await self._session.execute(
            select(SearchHistory)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.searched_at.desc())
            .limit(max_count)
        )
        return list(result.scalars().all())

    async def delete_keyword(self, user_id: str, keyword: str) -> bool:
        """
        특정 검색어를 삭제합니다.

        Args:
            user_id: 사용자 ID
            keyword: 삭제할 키워드

        Returns:
            삭제 성공 여부 (해당 키워드가 존재했으면 True)
        """
        result = await self._session.execute(
            delete(SearchHistory).where(
                SearchHistory.user_id == user_id,
                SearchHistory.keyword == keyword.strip(),
            )
        )
        return result.rowcount > 0

    async def delete_all(self, user_id: str) -> int:
        """
        사용자의 모든 검색 이력을 삭제합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            삭제된 항목 수
        """
        result = await self._session.execute(
            delete(SearchHistory).where(SearchHistory.user_id == user_id)
        )
        return result.rowcount

    async def _trim_old_records(self, user_id: str) -> None:
        """
        보관 건수를 초과하는 오래된 검색 이력을 삭제합니다.

        최대 보관 건수(RECENT_SEARCH_MAX, 기본 20)를 초과하면
        가장 오래된 항목부터 삭제합니다.

        Args:
            user_id: 사용자 ID
        """
        max_count = self._settings.RECENT_SEARCH_MAX

        # 현재 보유 건수 확인
        count_result = await self._session.execute(
            select(func.count(SearchHistory.id)).where(
                SearchHistory.user_id == user_id
            )
        )
        total = count_result.scalar() or 0

        if total <= max_count:
            return

        # 초과분의 가장 오래된 항목 ID 조회
        excess_count = total - max_count
        oldest_query = (
            select(SearchHistory.id)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.searched_at.asc())
            .limit(excess_count)
        )
        oldest_result = await self._session.execute(oldest_query)
        oldest_ids = [row[0] for row in oldest_result.fetchall()]

        # 오래된 항목 삭제
        if oldest_ids:
            await self._session.execute(
                delete(SearchHistory).where(SearchHistory.id.in_(oldest_ids))
            )
