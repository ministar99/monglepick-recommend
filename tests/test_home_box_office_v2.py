import pytest

from app.model.schema import MovieSearchResponse, PaginationMeta
from app.v2.model.dto import MovieDTO
from app.v2.service.search_service import SearchService


def _movie(
    movie_id: str,
    *,
    tmdb_id: int | None = None,
    imdb_id: str | None = None,
    kobis_movie_cd: str | None = None,
    kmdb_id: str | None = None,
    title: str | None = None,
    title_en: str | None = None,
    poster_path: str | None = None,
    overview: str | None = None,
    rating: float | None = None,
    vote_count: int | None = None,
    release_year: int | None = None,
    source: str | None = None,
) -> MovieDTO:
    return MovieDTO(
        movie_id=movie_id,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        kobis_movie_cd=kobis_movie_cd,
        kmdb_id=kmdb_id,
        title=title or movie_id,
        title_en=title_en,
        poster_path=poster_path,
        overview=overview,
        rating=rating,
        vote_count=vote_count,
        release_year=release_year,
        genres=["드라마"],
        source=source,
    )


class StubMovieRepository:
    def __init__(
        self,
        source_movies: list[MovieDTO],
        candidates_by_key: dict[str, list[MovieDTO]],
        title_candidates_by_key: dict[str, list[MovieDTO]] | None = None,
    ):
        self._source_movies = source_movies
        self._candidates_by_key = candidates_by_key
        self._title_candidates_by_key = title_candidates_by_key or {}
        self.source_limits: list[int] = []

    async def find_home_box_office_source_movies(self, limit: int = 120) -> list[MovieDTO]:
        self.source_limits.append(limit)
        return self._source_movies[:limit]

    async def find_by_identifiers(self, identifiers: list[str]) -> list[MovieDTO]:
        for identifier in identifiers:
            candidates = self._candidates_by_key.get(identifier)
            if candidates is not None:
                return candidates
        return []

    async def find_with_posters_by_title(
        self,
        *,
        title: str | None,
        title_en: str | None = None,
        limit: int = 20,
    ) -> list[MovieDTO]:
        for key in (title, title_en):
            if key and key in self._title_candidates_by_key:
                return self._title_candidates_by_key[key][:limit]
        return []

    async def find_with_posters_by_titles(
        self,
        titles: list[str],
        limit: int = 200,
    ) -> list[MovieDTO]:
        merged: dict[str, MovieDTO] = {}
        for title in titles:
            candidates = self._title_candidates_by_key.get(title, [])
            for candidate in candidates[:limit]:
                merged[candidate.movie_id] = candidate
        return list(merged.values())


class FakeRedis:
    def __init__(self, initial_data: dict[str, str] | None = None):
        self._data = initial_data or {}
        self.setex_calls: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._data[key] = value
        self.setex_calls.append((key, ttl, value))


@pytest.mark.asyncio
async def test_home_box_office_replaces_sparse_rerelease_with_richer_original():
    sparse_rerelease = _movie(
        "reissue-1",
        tmdb_id=101,
        imdb_id="tt101",
        kobis_movie_cd="K101",
        title="재개봉판",
        poster_path=None,
        overview=None,
        rating=7.1,
        vote_count=5,
        release_year=2026,
        source="kobis",
    )
    duplicate_sparse_rerelease = _movie(
        "reissue-2",
        tmdb_id=101,
        imdb_id="tt101",
        kobis_movie_cd="K101",
        title="재개봉판-중복",
        poster_path=None,
        overview=None,
        rating=7.0,
        vote_count=3,
        release_year=2026,
        source="kobis",
    )
    rich_original = _movie(
        "original-101",
        tmdb_id=101,
        imdb_id="tt101",
        kobis_movie_cd="K101",
        title="오리지널 영화",
        poster_path="/original-101.jpg",
        overview="풍부한 줄거리",
        rating=8.4,
        vote_count=1800,
        release_year=2010,
        source="tmdb",
    )
    another_movie = _movie(
        "movie-202",
        tmdb_id=202,
        imdb_id="tt202",
        title="다른 영화",
        poster_path="/movie-202.jpg",
        overview="다른 줄거리",
        rating=7.9,
        vote_count=900,
        release_year=2024,
        source="tmdb",
    )

    stub_repo = StubMovieRepository(
        source_movies=[sparse_rerelease, duplicate_sparse_rerelease, another_movie],
        candidates_by_key={
            "reissue-1": [sparse_rerelease, rich_original],
            "reissue-2": [duplicate_sparse_rerelease, rich_original],
            "movie-202": [another_movie],
        },
    )
    service = SearchService(conn=None)
    service._movie_repo = stub_repo

    result = await service.get_home_box_office_movies(page=1, size=2)

    assert [movie.movie_id for movie in result.movies] == ["original-101", "movie-202"]
    assert [movie.title for movie in result.movies] == ["오리지널 영화", "다른 영화"]
    assert result.pagination.total == 2
    assert result.search_source == "mysql"


@pytest.mark.asyncio
async def test_home_box_office_fills_requested_frames_from_later_candidates():
    source_movies = [
        _movie(
            f"movie-{index}",
            tmdb_id=index,
            title=f"영화 {index}",
            poster_path=f"/movie-{index}.jpg",
            overview=f"줄거리 {index}",
            rating=7.0 + (index / 100),
            vote_count=100 + index,
            release_year=2020 + (index % 5),
            source="tmdb",
        )
        for index in range(1, 13)
    ]

    stub_repo = StubMovieRepository(
        source_movies=source_movies,
        candidates_by_key={movie.movie_id: [movie] for movie in source_movies},
    )
    service = SearchService(conn=None)
    service._movie_repo = stub_repo

    result = await service.get_home_box_office_movies(page=1, size=12)

    assert len(result.movies) == 12
    assert result.movies[0].movie_id == "movie-1"
    assert result.movies[-1].movie_id == "movie-12"
    assert result.pagination.total == 12
    assert stub_repo.source_limits[-1] >= 96


@pytest.mark.asyncio
async def test_home_box_office_uses_title_fallback_when_identifier_match_has_no_poster():
    sparse_movie = _movie(
        "sparse-title-only",
        title="공동경비구역 JSA",
        title_en="Joint Security Area",
        poster_path=None,
        overview=None,
        rating=7.4,
        vote_count=12,
        release_year=2026,
        source="kobis",
    )
    title_matched_movie = _movie(
        "original-title-match",
        title="공동경비구역 JSA",
        title_en="Joint Security Area",
        poster_path="/jsa.jpg",
        overview="포스터가 있는 원본 영화",
        rating=8.1,
        vote_count=1500,
        release_year=2000,
        source="tmdb",
    )

    stub_repo = StubMovieRepository(
        source_movies=[sparse_movie],
        candidates_by_key={"sparse-title-only": [sparse_movie]},
        title_candidates_by_key={"공동경비구역 JSA": [title_matched_movie]},
    )
    service = SearchService(conn=None)
    service._movie_repo = stub_repo

    result = await service.get_home_box_office_movies(page=1, size=1)

    assert [movie.movie_id for movie in result.movies] == ["original-title-match"]
    assert result.movies[0].poster_url is not None


@pytest.mark.asyncio
async def test_home_box_office_returns_cached_response_first():
    cached_response = MovieSearchResponse(
        movies=[
            _movie(
                "cached-home-movie",
                title="캐시된 홈 영화",
                poster_path="/cached.jpg",
                overview="캐시된 응답",
                rating=8.2,
                vote_count=999,
                release_year=2025,
                source="tmdb",
            )
        ],
        pagination=PaginationMeta(page=1, size=12, total=1, total_pages=1),
        did_you_mean=None,
        related_queries=[],
        search_source="mysql",
    )
    redis = FakeRedis(
        {
            SearchService._home_box_office_cache_key(1, 12): cached_response.model_dump_json(),
        }
    )
    service = SearchService(conn=None, redis_client=redis)

    async def unexpected_find_home_box_office_source_movies(_: int = 120):
        raise AssertionError("cache hit 경로에서는 DB를 조회하면 안 됩니다.")

    service._movie_repo.find_home_box_office_source_movies = unexpected_find_home_box_office_source_movies

    response = await service.get_home_box_office_movies(page=1, size=12)

    assert [movie.movie_id for movie in response.movies] == ["cached-home-movie"]
    assert redis.setex_calls == []


@pytest.mark.asyncio
async def test_home_box_office_caches_final_response_with_daily_ttl():
    source_movie = _movie(
        "movie-1",
        tmdb_id=1,
        title="오늘의 인기 영화",
        poster_path="/movie-1.jpg",
        overview="최신 박스오피스 영화",
        rating=7.9,
        vote_count=320,
        release_year=2026,
        source="tmdb",
    )
    stub_repo = StubMovieRepository(
        source_movies=[source_movie],
        candidates_by_key={source_movie.movie_id: [source_movie]},
    )
    redis = FakeRedis()
    service = SearchService(conn=None, redis_client=redis)
    service._movie_repo = stub_repo

    response = await service.get_home_box_office_movies(page=1, size=12)

    assert [movie.movie_id for movie in response.movies] == ["movie-1"]
    assert len(redis.setex_calls) == 1
    cache_key, ttl, cached_payload = redis.setex_calls[0]
    assert cache_key == SearchService._home_box_office_cache_key(1, 12)
    assert ttl == service._settings.HOME_BOX_OFFICE_CACHE_TTL == 86400

    cached_response = MovieSearchResponse.model_validate_json(cached_payload)
    assert [movie.movie_id for movie in cached_response.movies] == ["movie-1"]
