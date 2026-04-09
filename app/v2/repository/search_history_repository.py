"""
검색 이력 리포지토리 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 SearchHistoryRepository를 Raw SQL로 재구현합니다.
검색 요청과 검색 결과 클릭 이벤트를 search_history 테이블에 저장합니다.
"""

import json
import logging
from datetime import datetime, timezone

import aiomysql

from app.config import get_settings
from app.v2.model.dto import SearchHistoryDTO

logger = logging.getLogger(__name__)


class SearchHistoryRepository:
    """검색 이력 CRUD 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        """
        Args:
            conn: aiomysql 비동기 커넥션
        """
        self._conn = conn
        self._settings = get_settings()

    async def add_search(
        self,
        user_id: str,
        keyword: str,
        result_count: int,
        filters: dict | None = None,
        clicked_movie_id: str | None = None,
    ) -> SearchHistoryDTO:
        """
        검색 이력 이벤트를 새 레코드로 추가합니다.

        Args:
            user_id: 사용자 ID
            keyword: 검색 키워드 (공백 제거 후 저장)
            result_count: 검색 결과 수
            filters: 검색 시 적용한 필터 정보
            clicked_movie_id: 검색 결과에서 클릭한 영화 ID

        Returns:
            저장된 SearchHistoryDTO
        """
        keyword_cleaned = keyword.strip()
        now = datetime.now(timezone.utc)
        filters_json = json.dumps(filters, ensure_ascii=False) if filters is not None else None

        insert_sql = (
            "INSERT INTO search_history "
            "(user_id, keyword, searched_at, result_count, clicked_movie_id, filters) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(
                insert_sql,
                (user_id, keyword_cleaned, now, result_count, clicked_movie_id, filters_json),
            )

        return SearchHistoryDTO(
            user_id=user_id,
            keyword=keyword_cleaned,
            searched_at=now,
            result_count=result_count,
            clicked_movie_id=clicked_movie_id,
            filters=filters,
        )

    async def get_recent(
        self, user_id: str, limit: int | None = None
    ) -> list[SearchHistoryDTO]:
        """
        사용자의 최근 검색어를 최신순으로 반환합니다.

        Args:
            user_id: 사용자 ID
            limit: 최대 반환 건수 (None이면 설정값 사용)

        Returns:
            최근 검색 이력 DTO 목록 (최신순 정렬)
        """
        max_count = limit or self._settings.RECENT_SEARCH_MAX
        sql = (
            "SELECT * FROM search_history "
            "WHERE user_id = %s "
            "ORDER BY searched_at DESC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            rows = await cur.fetchall()

        unique_rows: list[SearchHistoryDTO] = []
        seen_keywords: set[str] = set()

        for row in rows:
            keyword = row["keyword"]
            if keyword in seen_keywords:
                continue

            seen_keywords.add(keyword)
            unique_rows.append(SearchHistoryDTO(**row))
            if len(unique_rows) >= max_count:
                break

        return unique_rows

    async def delete_keyword(self, user_id: str, keyword: str) -> bool:
        """
        특정 검색어를 삭제합니다.

        Args:
            user_id: 사용자 ID
            keyword: 삭제할 키워드

        Returns:
            삭제 성공 여부 (해당 키워드가 존재했으면 True)
        """
        sql = (
            "DELETE FROM search_history "
            "WHERE user_id = %s AND keyword = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (user_id, keyword.strip()))
            return cur.rowcount > 0

    async def delete_all(self, user_id: str) -> int:
        """
        사용자의 모든 검색 이력을 삭제합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            삭제된 항목 수
        """
        sql = "DELETE FROM search_history WHERE user_id = %s"
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (user_id,))
            return cur.rowcount
