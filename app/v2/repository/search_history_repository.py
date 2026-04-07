"""
검색 이력 리포지토리 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 SearchHistoryRepository를 Raw SQL로 재구현합니다.
search_history 테이블에 대한 CRUD를 담당합니다.

동일 키워드 재검색 시 타임스탬프만 갱신하며,
최대 보관 건수(20건)를 초과하면 가장 오래된 항목을 삭제합니다.
"""

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

    async def add_search(self, user_id: str, keyword: str) -> SearchHistoryDTO:
        """
        검색 이력을 추가하거나 기존 키워드의 타임스탬프를 갱신합니다.

        동일 키워드가 이미 존재하면 searched_at만 현재 시각으로 갱신하고,
        새 키워드이면 INSERT합니다. 최대 보관 건수를 초과하면 오래된 것을 삭제합니다.

        Args:
            user_id: 사용자 ID
            keyword: 검색 키워드 (공백 제거 후 저장)

        Returns:
            저장/갱신된 SearchHistoryDTO
        """
        keyword_cleaned = keyword.strip()
        now = datetime.now(timezone.utc)

        # 기존 동일 키워드 검색
        select_sql = (
            "SELECT * FROM search_history "
            "WHERE user_id = %s AND keyword = %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(select_sql, (user_id, keyword_cleaned))
            existing = await cur.fetchone()

        if existing:
            # 기존 키워드: 타임스탬프만 갱신
            update_sql = (
                "UPDATE search_history SET searched_at = %s "
                "WHERE id = %s"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(update_sql, (now, existing["id"]))

            # 갱신된 레코드 반환
            existing["searched_at"] = now
            return SearchHistoryDTO(**existing)
        else:
            # 새 키워드: INSERT
            insert_sql = (
                "INSERT INTO search_history (user_id, keyword, searched_at) "
                "VALUES (%s, %s, %s)"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(insert_sql, (user_id, keyword_cleaned, now))
                new_id = cur.lastrowid

            # 최대 보관 건수 초과 시 오래된 항목 삭제
            await self._trim_old_records(user_id)

            return SearchHistoryDTO(
                id=new_id,
                user_id=user_id,
                keyword=keyword_cleaned,
                searched_at=now,
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
            "ORDER BY searched_at DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, max_count))
            rows = await cur.fetchall()

        return [SearchHistoryDTO(**row) for row in rows]

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
        count_sql = (
            "SELECT COUNT(*) AS total FROM search_history "
            "WHERE user_id = %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(count_sql, (user_id,))
            count_row = await cur.fetchone()

        total = count_row["total"] if count_row else 0
        if total <= max_count:
            return

        # 초과분의 가장 오래된 항목 ID 조회
        excess_count = total - max_count
        oldest_sql = (
            "SELECT id FROM search_history "
            "WHERE user_id = %s "
            "ORDER BY searched_at ASC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(oldest_sql, (user_id, excess_count))
            oldest_rows = await cur.fetchall()

        oldest_ids = [row["id"] for row in oldest_rows]

        # 오래된 항목 삭제
        if oldest_ids:
            placeholders = ", ".join(["%s"] * len(oldest_ids))
            delete_sql = f"DELETE FROM search_history WHERE id IN ({placeholders})"
            async with self._conn.cursor() as cur:
                await cur.execute(delete_sql, oldest_ids)
