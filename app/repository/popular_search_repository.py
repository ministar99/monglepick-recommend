"""
관리자 인기 검색어 오버레이 리포지토리

Backend JPA가 관리하는 popular_search_keyword 테이블을 읽어,
사용자 인기 검색어 노출 시 적용할 운영 메타를 제공합니다.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import PopularSearchKeyword


class PopularSearchRepository:
    """popular_search_keyword 조회 리포지토리"""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_all_keywords(self) -> list[PopularSearchKeyword]:
        """
        인기 검색어 오버레이 메타 전체를 조회합니다.

        테이블 규모가 작고 운영 메타가 드물어 전체 조회 후 서비스 레이어에서
        노출 후보를 선별합니다.
        """
        result = await self._session.execute(
            select(PopularSearchKeyword)
            .order_by(
                PopularSearchKeyword.display_rank.is_(None).asc(),
                PopularSearchKeyword.display_rank.asc(),
                PopularSearchKeyword.manual_priority.desc(),
                PopularSearchKeyword.id.asc(),
            )
        )
        return list(result.scalars().all())
