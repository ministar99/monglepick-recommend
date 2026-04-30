import pytest

from app.model.schema import MovieBrief, MovieSearchResponse, PaginationMeta
from app.search_elasticsearch import ESSearchMovieItem, ESSearchMoviesResult
from app.v2.model.dto import MovieDTO
from app.v2.service.personalized_search_service import PersonalizedSearchService


def _movie(
    movie_id: str,
    *,
    title: str,
    genres: list[str],
    director: str | None = None,
    cast: list[str] | None = None,
    collection_name: str | None = None,
    rating: float | None = None,
    vote_count: int | None = None,
    release_year: int | None = None,
    poster_path: str | None = "/poster.jpg",
    overview: str | None = "충분히 긴 줄거리 설명입니다.",
) -> MovieDTO:
    return MovieDTO(
        movie_id=movie_id,
        title=title,
        title_en=None,
        genres=genres,
        director=director,
        cast_members=cast or [],
        collection_name=collection_name,
        rating=rating,
        vote_count=vote_count,
        release_year=release_year,
        poster_path=poster_path,
        overview=overview,
    )


class StubFavoriteGenreRepository:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def list_selected_by_user(self, user_id: str) -> list[dict]:
        return self._rows


class StubFavoriteMovieRepository:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def list_by_user(self, user_id: str) -> list[dict]:
        return self._rows


class StubWishlistRepository:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def list_by_user(self, user_id: str, offset: int, limit: int) -> list[dict]:
        return self._rows[:limit]


class StubReviewRepository:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def list_by_user(self, user_id: str, offset: int, limit: int) -> list[dict]:
        return self._rows[:limit]


class StubPersonalizedRepository:
    def __init__(
        self,
        *,
        implicit_ids: list[str] | None = None,
        watched_ids: list[str] | None = None,
        dismissed_ids: list[str] | None = None,
        behavior_profile: dict | None = None,
    ):
        self._implicit_ids = implicit_ids or []
        self._watched_ids = watched_ids or []
        self._dismissed_ids = dismissed_ids or []
        self._behavior_profile = behavior_profile or {}

    async def list_top_implicit_movie_ids(self, user_id: str, limit: int = 6) -> list[str]:
        return self._implicit_ids[:limit]

    async def list_watched_movie_ids(self, user_id: str, limit: int = 80) -> list[str]:
        return self._watched_ids[:limit]

    async def list_dismissed_movie_ids(self, user_id: str, limit: int = 200) -> list[str]:
        return self._dismissed_ids[:limit]

    async def get_behavior_profile(self, user_id: str) -> dict:
        return self._behavior_profile


class StubMovieRepository:
    def __init__(
        self,
        *,
        movies_by_id: dict[str, MovieDTO],
        search_results: dict[tuple[str, str], list[MovieDTO]] | None = None,
        collection_results: dict[str, list[MovieDTO]] | None = None,
        title_candidates_by_key: dict[str, list[MovieDTO]] | None = None,
    ):
        self._movies_by_id = movies_by_id
        self._search_results = search_results or {}
        self._collection_results = collection_results or {}
        self._title_candidates_by_key = title_candidates_by_key or {}

    async def find_by_ids(self, movie_ids: list[str]) -> list[MovieDTO]:
        return [self._movies_by_id[movie_id] for movie_id in movie_ids if movie_id in self._movies_by_id]

    async def find_by_collection_name(
        self,
        collection_name: str,
        *,
        exclude_movie_id: str | None = None,
    ) -> list[MovieDTO]:
        movies = list(self._collection_results.get(collection_name, []))
        if exclude_movie_id:
            movies = [movie for movie in movies if movie.movie_id != exclude_movie_id]
        return movies

    async def search(
        self,
        keyword: str | None = None,
        search_type: str = "title",
        genre: str | None = None,
        genres: list[str] | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        rating_min: float | None = None,
        rating_max: float | None = None,
        popularity_min: float | None = None,
        popularity_max: float | None = None,
        vote_count_min: int | None = None,
        sort_by: str = "rating",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
        **kwargs,
    ) -> tuple[list[MovieDTO], int]:
        if genres:
            key = ("genres", ",".join(genres))
        else:
            key = (search_type, keyword or "")
        movies = list(self._search_results.get(key, []))[:size]
        return movies, len(movies)

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


class StubMatchCowatchService:
    def __init__(self, rows_by_pair: dict[tuple[str, str], list[dict]] | None = None):
        self._rows_by_pair = rows_by_pair or {}

    async def get_cowatched_candidates(
        self,
        movie_id_1: str,
        movie_id_2: str,
        top_k: int = 20,
        rating_threshold: float = 4.0,
    ) -> list[dict]:
        key = tuple(sorted((movie_id_1, movie_id_2)))
        return self._rows_by_pair.get(key, [])[:top_k]


class StubSearchService:
    def __init__(self, movies: list):
        self._movies = movies

    async def get_home_box_office_movies(self, page: int = 1, size: int = 12) -> MovieSearchResponse:
        return MovieSearchResponse(
            movies=self._movies[:size],
            pagination=PaginationMeta(
                page=page,
                size=size,
                total=len(self._movies),
                total_pages=1,
            ),
            did_you_mean=None,
            related_queries=[],
            search_source="mysql",
        )


class StubSearchEs:
    def __init__(
        self,
        *,
        available: bool = False,
        genre_results: dict[str, list[ESSearchMovieItem]] | None = None,
        related_results: dict[str, list[ESSearchMovieItem] | None] | None = None,
    ):
        self._available = available
        self._genre_results = genre_results or {}
        self._related_results = related_results or {}

    def is_available(self) -> bool:
        return self._available

    async def search_movies(self, *, genres: list[str] | None = None, size: int = 20, **kwargs):
        genre_key = ",".join(genres or [])
        movies = list(self._genre_results.get(genre_key, []))[:size]
        return ESSearchMoviesResult(
            movies=movies,
            total=len(movies),
            did_you_mean=None,
            related_queries=[],
        )

    async def search_related_movies(self, *, movie_id: str, limit: int | None = None, **kwargs):
        movies = self._related_results.get(movie_id, [])
        if movies is None:
            return None
        normalized_limit = limit or len(movies)
        return list(movies)[:normalized_limit]


@pytest.mark.asyncio
async def test_personalized_top_picks_ranks_hybrid_candidates_and_excludes_seen_movies():
    favorite_movie = _movie(
        "fav-1",
        title="인셉션",
        genres=["SF", "스릴러"],
        director="크리스토퍼 놀란",
        cast=["레오나르도 디카프리오"],
        rating=8.8,
        vote_count=3200,
        release_year=2010,
    )
    review_seed = _movie(
        "review-1",
        title="라라랜드",
        genres=["드라마", "로맨스"],
        director="데이미언 셔젤",
        cast=["엠마 스톤"],
        rating=8.0,
        vote_count=2100,
        release_year=2016,
    )
    wishlist_seed = _movie(
        "wish-1",
        title="어바웃 타임",
        genres=["로맨스", "드라마"],
        director="리처드 커티스",
        cast=["도널 글리슨"],
        rating=7.9,
        vote_count=1500,
        release_year=2013,
    )
    interstellar = _movie(
        "pick-1",
        title="인터스텔라",
        genres=["SF", "드라마"],
        director="크리스토퍼 놀란",
        cast=["매튜 맥커너히"],
        rating=8.7,
        vote_count=4100,
        release_year=2014,
    )
    dune = _movie(
        "pick-2",
        title="듄",
        genres=["SF", "모험"],
        director="드니 빌뇌브",
        cast=["티모시 샬라메"],
        rating=8.1,
        vote_count=2800,
        release_year=2021,
    )
    parasite = _movie(
        "pick-3",
        title="기생충",
        genres=["드라마", "스릴러"],
        director="봉준호",
        cast=["송강호"],
        rating=8.6,
        vote_count=3900,
        release_year=2019,
    )
    whiplash = _movie(
        "pick-4",
        title="위플래쉬",
        genres=["드라마", "음악"],
        director="데이미언 셔젤",
        cast=["마일스 텔러"],
        rating=8.4,
        vote_count=2400,
        release_year=2014,
    )
    dismissed = _movie(
        "dismissed-1",
        title="셔터 아일랜드",
        genres=["스릴러"],
        director="마틴 스코세이지",
        cast=["레오나르도 디카프리오"],
        rating=8.2,
        vote_count=2300,
        release_year=2010,
    )

    service = PersonalizedSearchService(conn=None)
    service._favorite_genre_repo = StubFavoriteGenreRepository(
        [
            {"genre_name": "SF"},
            {"genre_name": "드라마"},
        ]
    )
    service._favorite_movie_repo = StubFavoriteMovieRepository(
        [{"movie_id": "fav-1"}]
    )
    service._wishlist_repo = StubWishlistRepository(
        [{"movie_id": "wish-1"}]
    )
    service._review_repo = StubReviewRepository(
        [{"movie_id": "review-1", "rating": 5.0, "created_at": "2026-04-30T12:00:00"}]
    )
    service._personalized_repo = StubPersonalizedRepository(
        watched_ids=["review-1"],
        dismissed_ids=["dismissed-1"],
        behavior_profile={
            "taste_consistency": 0.82,
            "genre_affinity": {"SF": 0.9, "드라마": 0.5},
        },
    )
    service._movie_repo = StubMovieRepository(
        movies_by_id={
            "fav-1": favorite_movie,
            "review-1": review_seed,
            "wish-1": wishlist_seed,
            "pick-1": interstellar,
            "pick-2": dune,
            "pick-3": parasite,
            "pick-4": whiplash,
            "dismissed-1": dismissed,
        },
        search_results={
            ("genres", "SF"): [interstellar, dune],
            ("genres", "드라마"): [parasite, whiplash],
            ("director", "크리스토퍼 놀란"): [interstellar],
            ("actor", "레오나르도 디카프리오"): [dismissed],
            ("director", "데이미언 셔젤"): [whiplash],
            ("actor", "엠마 스톤"): [whiplash],
            ("director", "리처드 커티스"): [],
            ("actor", "도널 글리슨"): [],
        },
    )
    service._match_cowatch_service = StubMatchCowatchService(
        {
            ("fav-1", "review-1"): [
                {"movie_id": "pick-3", "cf_score": 0.94, "co_user_count": 5},
            ],
        }
    )
    service._search_service = StubSearchService([])
    service._search_es = StubSearchEs(available=False)

    result = await service.get_top_picks(user_id="user-1", limit=5)

    movie_ids = [movie.movie_id for movie in result.movies]
    assert movie_ids[0] in {"pick-1", "pick-3"}
    assert "pick-1" in movie_ids
    assert "pick-3" in movie_ids
    assert "dismissed-1" not in movie_ids
    assert "review-1" not in movie_ids
    assert "wish-1" not in movie_ids
    assert result.total_candidates >= len(result.movies)
    assert any("선호 장르" in reason for reason in result.movies[0].personalized_reasons)
    assert any(
        ("감독 작품" in reason) or ("비슷한 취향" in reason) or ("결이 비슷한 작품" in reason)
        for movie in result.movies
        for reason in movie.personalized_reasons
    )


@pytest.mark.asyncio
async def test_personalized_top_picks_falls_back_to_box_office_when_user_signal_is_empty():
    service = PersonalizedSearchService(conn=None)
    service._favorite_genre_repo = StubFavoriteGenreRepository([])
    service._favorite_movie_repo = StubFavoriteMovieRepository([])
    service._wishlist_repo = StubWishlistRepository([])
    service._review_repo = StubReviewRepository([])
    service._personalized_repo = StubPersonalizedRepository()
    service._movie_repo = StubMovieRepository(movies_by_id={})
    service._match_cowatch_service = StubMatchCowatchService()
    service._search_service = StubSearchService(
        [
            MovieBrief(
                movie_id="box-1",
                title="매드 맥스: 분노의 도로",
                title_en=None,
                genres=["액션"],
                rating=8.2,
                vote_count=3000,
                release_year=2015,
                poster_url="https://image.tmdb.org/t/p/w500/box-1.jpg",
                trailer_url=None,
                overview="질주하는 생존 서사입니다.",
            ),
            MovieBrief(
                movie_id="box-2",
                title="인사이드 아웃 2",
                title_en=None,
                genres=["애니메이션", "가족"],
                rating=7.8,
                vote_count=1800,
                release_year=2024,
                poster_url="https://image.tmdb.org/t/p/w500/box-2.jpg",
                trailer_url=None,
                overview="감정들의 새로운 모험입니다.",
            ),
        ]
    )
    service._search_es = StubSearchEs(available=False)

    result = await service.get_top_picks(user_id="user-2", limit=2)

    assert [movie.movie_id for movie in result.movies] == ["box-1", "box-2"]
    assert all(
        "최근 많이 보는 인기작이에요" in movie.personalized_reasons
        for movie in result.movies
    )


@pytest.mark.asyncio
async def test_personalized_top_picks_uses_es_candidates_when_available():
    favorite_movie = _movie(
        "fav-es-1",
        title="매트릭스",
        genres=["SF", "액션"],
        director="워쇼스키",
        cast=["키아누 리브스"],
        rating=8.7,
        vote_count=3300,
        release_year=1999,
    )
    interstellar_es = ESSearchMovieItem(
        movie_id="pick-es-1",
        title="인터스텔라",
        title_en=None,
        genres=["SF", "드라마"],
        release_year=2014,
        rating=8.7,
        vote_count=4100,
        poster_path="/interstellar.jpg",
        trailer_url=None,
        overview="우주를 배경으로 한 장대한 탐험입니다.",
        director="크리스토퍼 놀란",
        cast=["매튜 맥커너히"],
        keywords=[],
        collection_name=None,
    )
    dune_es = ESSearchMovieItem(
        movie_id="pick-es-2",
        title="듄",
        title_en=None,
        genres=["SF", "모험"],
        release_year=2021,
        rating=8.1,
        vote_count=2800,
        poster_path="/dune.jpg",
        trailer_url=None,
        overview="사막 행성에서 펼쳐지는 거대한 서사입니다.",
        director="드니 빌뇌브",
        cast=["티모시 샬라메"],
        keywords=[],
        collection_name=None,
    )

    service = PersonalizedSearchService(conn=None)
    service._favorite_genre_repo = StubFavoriteGenreRepository(
        [{"genre_name": "SF"}]
    )
    service._favorite_movie_repo = StubFavoriteMovieRepository(
        [{"movie_id": "fav-es-1"}]
    )
    service._wishlist_repo = StubWishlistRepository([])
    service._review_repo = StubReviewRepository([])
    service._personalized_repo = StubPersonalizedRepository()
    service._movie_repo = StubMovieRepository(
        movies_by_id={"fav-es-1": favorite_movie},
    )
    service._match_cowatch_service = StubMatchCowatchService()
    service._search_service = StubSearchService([])
    service._search_es = StubSearchEs(
        available=True,
        genre_results={"SF": [dune_es]},
        related_results={"fav-es-1": [interstellar_es]},
    )

    result = await service.get_top_picks(user_id="user-es", limit=3)

    assert [movie.movie_id for movie in result.movies][:2] == ["pick-es-1", "pick-es-2"]
    assert any(
        "결이 비슷한 작품" in reason
        for reason in result.movies[0].personalized_reasons
    )


@pytest.mark.asyncio
async def test_personalized_top_picks_replaces_invalid_poster_candidates_by_exact_title():
    favorite_movie = _movie(
        "fav-es-1",
        title="매트릭스",
        genres=["SF", "액션"],
        director="워쇼스키",
        cast=["키아누 리브스"],
        rating=8.7,
        vote_count=3300,
        release_year=1999,
    )
    interstellar_fallback = _movie(
        "pick-db-1",
        title="인터스텔라",
        genres=["SF", "드라마"],
        director="크리스토퍼 놀란",
        cast=["매튜 맥커너히"],
        rating=8.7,
        vote_count=4100,
        release_year=2014,
        poster_path="/interstellar.jpg",
    )
    invalid_interstellar_es = ESSearchMovieItem(
        movie_id="pick-es-1",
        title="인터스텔라",
        title_en=None,
        genres=["SF", "드라마"],
        release_year=2014,
        rating=8.7,
        vote_count=4100,
        poster_path="http://file.koreafilm.or.kr/thm/02/99/17/97/tn_DPF026925.jpg",
        trailer_url=None,
        overview="우주를 배경으로 한 장대한 탐험입니다.",
        director="크리스토퍼 놀란",
        cast=["매튜 맥커너히"],
        keywords=[],
        collection_name=None,
    )
    missing_poster_es = ESSearchMovieItem(
        movie_id="pick-es-2",
        title="포스터 없는 후보",
        title_en=None,
        genres=["SF"],
        release_year=2020,
        rating=7.1,
        vote_count=120,
        poster_path=None,
        trailer_url=None,
        overview="포스터가 없는 ES 후보입니다.",
        director=None,
        cast=[],
        keywords=[],
        collection_name=None,
    )

    service = PersonalizedSearchService(conn=None)
    service._favorite_genre_repo = StubFavoriteGenreRepository([])
    service._favorite_movie_repo = StubFavoriteMovieRepository(
        [{"movie_id": "fav-es-1"}]
    )
    service._wishlist_repo = StubWishlistRepository([])
    service._review_repo = StubReviewRepository([])
    service._personalized_repo = StubPersonalizedRepository()
    service._movie_repo = StubMovieRepository(
        movies_by_id={
            "fav-es-1": favorite_movie,
            "pick-db-1": interstellar_fallback,
        },
        title_candidates_by_key={
            "인터스텔라": [interstellar_fallback],
        },
    )
    service._match_cowatch_service = StubMatchCowatchService()
    service._search_service = StubSearchService([])
    service._search_es = StubSearchEs(
        available=True,
        related_results={
            "fav-es-1": [invalid_interstellar_es, missing_poster_es],
        },
    )

    result = await service.get_top_picks(user_id="user-poster-fix", limit=3)

    assert [movie.movie_id for movie in result.movies] == ["pick-db-1"]
    assert result.movies[0].poster_url == (
        f"{service._settings.TMDB_IMAGE_BASE_URL}/interstellar.jpg"
    )
