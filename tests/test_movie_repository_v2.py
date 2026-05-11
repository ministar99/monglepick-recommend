import pytest

from app.v2.repository.movie_repository import MovieRepository


class _RecordingCursor:
    def __init__(self):
        self.executions: list[tuple[str, list | tuple | None]] = []

    async def execute(self, sql, params=None):
        self.executions.append((sql, params))

    async def fetchall(self):
        return []

    async def fetchone(self):
        return {"total": 0}


class _CursorContext:
    def __init__(self, cursor):
        self._cursor = cursor

    async def __aenter__(self):
        return self._cursor

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _RecordingConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, *_args, **_kwargs):
        return _CursorContext(self._cursor)


@pytest.mark.asyncio
async def test_search_v2_requires_all_selected_genre_groups():
    cursor = _RecordingCursor()
    repo = MovieRepository(_RecordingConn(cursor))

    await repo.search(
        genre_match_groups=[["액션", "액숀"], ["드라마"]],
        sort_by="relevance",
        sort_order="desc",
        page=1,
        size=20,
    )

    select_sql, select_params = cursor.executions[0]
    count_sql, count_params = cursor.executions[1]

    assert "(JSON_CONTAINS(genres, JSON_QUOTE(%s)) OR JSON_CONTAINS(genres, JSON_QUOTE(%s)))" in select_sql
    assert "AND (JSON_CONTAINS(genres, JSON_QUOTE(%s)))" in select_sql
    assert select_params[3:6] == ["액션", "액숀", "드라마"]

    assert "(JSON_CONTAINS(genres, JSON_QUOTE(%s)) OR JSON_CONTAINS(genres, JSON_QUOTE(%s)))" in count_sql
    assert "AND (JSON_CONTAINS(genres, JSON_QUOTE(%s)))" in count_sql
    assert count_params[3:6] == ["액션", "액숀", "드라마"]


@pytest.mark.asyncio
async def test_search_v2_relevance_uses_genre_recommendation_order_sql():
    cursor = _RecordingCursor()
    repo = MovieRepository(_RecordingConn(cursor))

    await repo.search(
        genre_match_groups=[["액션"], ["드라마"]],
        sort_by="relevance",
        sort_order="desc",
        page=1,
        size=20,
    )

    select_sql, select_params = cursor.executions[0]

    assert "COALESCE(popularity_score, 0)" in select_sql
    assert "COALESCE(vote_count, 0)" in select_sql
    assert "WHEN release_year IS NULL THEN 0.0" in select_sql
    assert "rating DESC" in select_sql
    assert "vote_count DESC" in select_sql
    assert len(select_params) > 10
