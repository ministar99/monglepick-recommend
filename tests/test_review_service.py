from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.v2.service.review_service import ReviewService


def _make_review_row(*, liked):
    return {
        "id": 101,
        "user_id": "author_1",
        "movie_id": "movie_1",
        "movie_title": "테스트 영화",
        "poster_path": "/poster.jpg",
        "rating": 4.5,
        "content": "좋은 영화였습니다.",
        "author_nickname": "테스터",
        "is_spoiler": 0,
        "review_source": None,
        "review_category_code": None,
        "created_at": datetime.now(timezone.utc),
        "like_count": 7,
        "liked": liked,
    }


@pytest.mark.asyncio
async def test_get_reviews_리뷰좋아요상태를_응답에_포함한다():
    service = ReviewService(conn=None)
    service._repo = AsyncMock()
    service._repo.list_by_movie.return_value = [_make_review_row(liked=1)]
    service._repo.count_by_movie.return_value = 1

    result = await service.get_reviews("movie_1", user_id="user_1")

    service._repo.list_by_movie.assert_awaited_once_with(
        "movie_1",
        0,
        20,
        "latest",
        current_user_id="user_1",
    )
    assert result.total == 1
    assert len(result.reviews) == 1
    assert result.reviews[0].liked is True
    assert result.reviews[0].like_count == 7


@pytest.mark.asyncio
async def test_get_user_reviews_BIT값도_좋아요상태로_정규화한다():
    service = ReviewService(conn=None)
    service._repo = AsyncMock()
    service._repo.list_by_user.return_value = [_make_review_row(liked=b"\x01")]
    service._repo.count_by_user.return_value = 1

    result = await service.get_user_reviews("user_1")

    service._repo.list_by_user.assert_awaited_once_with(
        "user_1",
        0,
        20,
        current_user_id="user_1",
    )
    assert result.pagination.total == 1
    assert len(result.reviews) == 1
    assert result.reviews[0].liked is True
