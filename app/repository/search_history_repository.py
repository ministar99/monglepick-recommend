"""
검색 이력 리포지토리

검색 요청과 검색 결과 클릭 이벤트를 search_history 테이블에 저장합니다.
최근 검색어 화면에서는 최신 키워드만 추려서 반환합니다.
"""

from datetime import datetime, timezone

from sqlalchemy import delete, select
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

    async def add_search(
        self,
        user_id: str,
        keyword: str,
        result_count: int,
        filters: dict | None = None,
        clicked_movie_id: str | None = None,
    ) -> SearchHistory:
        """
        검색 이력 이벤트를 새 레코드로 추가합니다.

        Args:
            user_id: 사용자 ID
            keyword: 검색 키워드 (공백 제거 후 저장)
            result_count: 검색 결과 수
            filters: 검색 시 적용한 필터 정보
            clicked_movie_id: 검색 결과에서 클릭한 영화 ID

        Returns:
            저장된 SearchHistory 엔티티
        """
        keyword_cleaned = keyword.strip()
        new_record = SearchHistory(
            user_id=user_id,
            keyword=keyword_cleaned,
            searched_at=datetime.now(timezone.utc),
            result_count=result_count,
            clicked_movie_id=clicked_movie_id,
            filters=filters,
        )
        self._session.add(new_record)
        await self._session.flush()
        return new_record

    async def get_recent(
        self, user_id: str, limit: int | None = None
    ) -> list[SearchHistory]:
        """
        사용자의 최근 검색어를 최신순으로 반환합니다.

        동일 키워드는 가장 최근 레코드만 노출합니다.

        Args:
            user_id: 사용자 ID
            limit: 최대 반환 건수 (None이면 설정값 사용)

        Returns:
            최근 검색 이력 목록 (최신순 정렬)
        """
        records, _ = await self.get_recent_page(user_id=user_id, offset=0, limit=limit)
        return records

    async def get_recent_page(
        self,
        user_id: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> tuple[list[SearchHistory], bool]:
        """
        사용자의 최근 검색어를 페이지 단위로 반환합니다.

        중복 키워드는 가장 최근 레코드 1건만 남기고,
        offset은 "중복 제거가 끝난 목록" 기준으로 계산합니다.

        Args:
            user_id: 사용자 ID
            offset: 중복 제거된 목록 기준 시작 위치
            limit: 최대 반환 건수 (None이면 설정값 사용)

        Returns:
            tuple[list[SearchHistory], bool]:
                - 현재 페이지의 최근 검색 이력 목록
                - 다음 페이지 존재 여부
        """
        start_offset = max(0, offset)
        max_count = min(limit or self._settings.RECENT_SEARCH_MAX, self._settings.RECENT_SEARCH_MAX)
        result = await self._session.execute(
            select(SearchHistory)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.searched_at.desc())
        )

        unique_records: list[SearchHistory] = []
        seen_keywords: set[str] = set()
        skipped_unique_count = 0

        for record in result.scalars():
            if record.keyword in seen_keywords:
                continue

            seen_keywords.add(record.keyword)

            # offset 이전의 고유 키워드는 페이지 계산을 위해 건너뜁니다.
            if skipped_unique_count < start_offset:
                skipped_unique_count += 1
                continue

            unique_records.append(record)
            if len(unique_records) > max_count:
                break

        has_more = len(unique_records) > max_count
        return unique_records[:max_count], has_more

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
