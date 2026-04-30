from datetime import date

import pytest

from app.search_elasticsearch import ESSearchMovieItem, ESSearchMoviesResult
from app.v2.model.dto import MovieDTO
from app.v2.service.search_service import SearchService


def test_movie_detail_v2_prefers_release_date_column_without_fake_january_first():
    """v2 상세 응답도 release_date 컬럼을 우선 사용하고, 연도만으로 1월 1일을 만들지 않습니다."""
    service = SearchService(conn=None)
    dated_movie = MovieDTO(
        movie_id="1368337",
        title="개봉일 있는 영화",
        release_year=2026,
        release_date=date(2026, 4, 30),
    )
    year_only_movie = MovieDTO(
        movie_id="year-only",
        title="연도만 있는 영화",
        release_year=2026,
    )

    dated_detail = service._to_movie_detail(dated_movie)
    year_only_detail = service._to_movie_detail(year_only_movie)

    assert dated_detail.release_date == "2026-04-30"
    assert year_only_detail.release_date is None


class StubMovieRepository:
    def __init__(
        self,
        *,
        title_candidates_by_key: dict[str, list[MovieDTO]] | None = None,
    ):
        self._title_candidates_by_key = title_candidates_by_key or {}

    async def search(self, **kwargs):
        return [], 0

    async def find_with_posters_by_titles(
        self,
        titles: list[str],
        limit: int = 200,
    ) -> list[MovieDTO]:
        merged: dict[str, MovieDTO] = {}
        for title in titles:
            for movie in self._title_candidates_by_key.get(title, [])[:limit]:
                merged[movie.movie_id] = movie
        return list(merged.values())


class StubSearchEs:
    def __init__(self, movies: list[ESSearchMovieItem]):
        self._movies = movies

    async def search_movies(self, **kwargs):
        return ESSearchMoviesResult(
            movies=self._movies,
            total=len(self._movies),
            did_you_mean=None,
            related_queries=[],
        )


@pytest.mark.asyncio
async def test_search_movies_replaces_external_poster_results_with_exact_title_match():
    service = SearchService(conn=None)
    external_poster_movie = ESSearchMovieItem(
        movie_id="es-1",
        title="올드보이",
        title_en=None,
        genres=["스릴러", "드라마"],
        release_year=2003,
        rating=8.3,
        vote_count=1800,
        poster_path="http://file.koreafilm.or.kr/thm/02/99/17/97/tn_DPF026925.jpg",
        trailer_url=None,
        overview="복수극을 그린 작품",
        director="박찬욱",
        cast=["최민식"],
        keywords=[],
        collection_name=None,
    )
    title_matched_movie = MovieDTO(
        movie_id="db-1",
        title="올드보이",
        genres=["스릴러", "드라마"],
        release_year=2003,
        rating=8.4,
        vote_count=2500,
        poster_path="/oldboy.jpg",
        overview="포스터가 있는 동일 제목 영화",
    )
    service._search_es = StubSearchEs([external_poster_movie])
    service._movie_repo = StubMovieRepository(
        title_candidates_by_key={"올드보이": [title_matched_movie]}
    )

    result = await service.search_movies(keyword="올드보이", page=1, size=20)

    assert [movie.movie_id for movie in result.movies] == ["db-1"]
    assert result.movies[0].poster_url == f"{service._settings.TMDB_IMAGE_BASE_URL}/oldboy.jpg"
