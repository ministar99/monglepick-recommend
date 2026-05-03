"""
관리자 인기 검색어 오버레이 리포지토리 (v2 Raw SQL)

popular_search_keyword 테이블을 읽어 사용자 노출용 인기 검색어에 반영할
운영 메타를 제공합니다.
"""

import aiomysql

from app.v2.model.dto import PopularSearchKeywordDTO


class PopularSearchRepository:
    """popular_search_keyword 조회 리포지토리 (Raw SQL)"""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def get_all_keywords(self) -> list[PopularSearchKeywordDTO]:
        """인기 검색어 오버레이 메타 전체를 조회합니다."""
        sql = (
            "SELECT id, keyword, display_rank, manual_priority, is_excluded, "
            "admin_note, created_at, updated_at "
            "FROM popular_search_keyword "
            "ORDER BY display_rank IS NULL ASC, display_rank ASC, manual_priority DESC, id ASC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()

        return [PopularSearchKeywordDTO(**row) for row in rows]
