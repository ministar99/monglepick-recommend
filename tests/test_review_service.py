from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.v2.repository.review_repository import ReviewRepository
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


class _FakeCursor:
    def __init__(self, row):
        self._row = row
        self.executed_sql = None
        self.executed_params = None

    async def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = params

    async def fetchone(self):
        return self._row


class _FakeCursorContext:
    def __init__(self, cursor):
        self._cursor = cursor

    async def __aenter__(self):
        return self._cursor

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, *_args, **_kwargs):
        return _FakeCursorContext(self._cursor)


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


@pytest.mark.asyncio
async def test_should_refresh_personalized_profile_사용자평균보다_높은평점만_반영한다():
    service = ReviewService(conn=None)
    service._repo = AsyncMock()
    service._repo.get_user_average_rating.return_value = 3.8

    assert await service.should_refresh_personalized_profile(
        user_id="user_1",
        rating=4.5,
    ) is True
    assert await service.should_refresh_personalized_profile(
        user_id="user_1",
        rating=3.5,
    ) is False


@pytest.mark.asyncio
async def test_should_refresh_personalized_profile_평균이없으면_4점이상만_반영한다():
    service = ReviewService(conn=None)
    service._repo = AsyncMock()
    service._repo.get_user_average_rating.return_value = None

    assert await service.should_refresh_personalized_profile(
        user_id="user_1",
        rating=4.0,
    ) is True
    assert await service.should_refresh_personalized_profile(
        user_id="user_1",
        rating=3.5,
    ) is False


@pytest.mark.asyncio
async def test_exists_by_user_movie_소프트삭제된리뷰는_중복검사에서_제외한다():
    fake_cursor = _FakeCursor(row=None)
    repo = ReviewRepository(conn=_FakeConn(fake_cursor))
    repo._get_columns = AsyncMock(return_value={"review_id", "user_id", "movie_id", "is_deleted"})

    result = await repo.exists_by_user_movie("user_1", "movie_1")

    assert result is False
    repo._get_columns.assert_awaited_once_with("reviews")
    assert "COALESCE(is_deleted, 0) = 0" in fake_cursor.executed_sql
    assert fake_cursor.executed_params == ("user_1", "movie_1")
