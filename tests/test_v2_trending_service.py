from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.v2.model.dto import PopularSearchKeywordDTO, TrendingKeywordDTO
from app.v2.service.trending_service import TrendingService


@pytest.mark.asyncio
async def test_admin_popular_keywords_uses_trending_count_for_search_count():
    """관리자 인기 검색어는 trending_keywords 검색 수를 유지하고 전환율만 search_history로 계산합니다."""
    service = TrendingService(conn=MagicMock(), redis_client=MagicMock())
    now = datetime.now(timezone.utc)

    service._trending_repo.get_recent_top_keywords = AsyncMock(return_value=[
        TrendingKeywordDTO(
            id=1,
            keyword="인터스텔라",
            search_count=30,
            last_searched_at=now,
        ),
        TrendingKeywordDTO(
            id=2,
            keyword="기생충",
            search_count=10,
            last_searched_at=now,
        ),
    ])
    service._history_repo.get_keyword_stats_since = AsyncMock(return_value={
        "인터스텔라": {"search_count": 2, "click_count": 1},
        "기생충": {"search_count": 50, "click_count": 25},
    })

    response = await service.get_admin_popular_keywords(period="1d", limit=2)

    assert [item.keyword for item in response.keywords] == ["인터스텔라", "기생충"]
    assert response.keywords[0].search_count == 30
    assert response.keywords[0].conversion_rate == 0.5
    assert response.keywords[1].search_count == 10
    assert response.keywords[1].conversion_rate == 0.5


@pytest.mark.asyncio
async def test_trending_applies_popular_keyword_overlay():
    """v2 인기 검색어도 관리자 운영 메타를 반영해 최종 순위를 계산합니다."""
    redis_client = MagicMock()
    redis_client.zrevrange = AsyncMock(return_value=[
        ("인터스텔라", 30),
        ("기생충", 20),
        ("라라랜드", 10),
    ])
    service = TrendingService(conn=MagicMock(), redis_client=redis_client)

    service._popular_search_repo.get_all_keywords = AsyncMock(return_value=[
        PopularSearchKeywordDTO(
            id=1,
            keyword="라라랜드",
            display_rank=1,
            manual_priority=0,
            is_excluded=False,
        ),
        PopularSearchKeywordDTO(
            id=2,
            keyword="기생충",
            display_rank=None,
            manual_priority=0,
            is_excluded=True,
        ),
        PopularSearchKeywordDTO(
            id=3,
            keyword="듄",
            display_rank=None,
            manual_priority=40,
            is_excluded=False,
        ),
    ])

    response = await service.get_trending()

    assert [item.keyword for item in response.keywords[:3]] == ["라라랜드", "듄", "인터스텔라"]
    assert response.keywords[0].search_count == 10
    assert response.keywords[1].search_count == 0
    assert all(item.keyword != "기생충" for item in response.keywords)
