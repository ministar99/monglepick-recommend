import pytest
from unittest.mock import AsyncMock

from app.model.schema import RelatedMovieItem, RelatedMoviesResponse
from app.search_elasticsearch import ESSearchMovieItem
from app.v2.model.dto import MovieDTO
from app.v2.service.related_movie_service import RelatedCandidate, RelatedMovieService


class FakeRedis:
    def __init__(self, initial_data: dict[str, str] | None = None):
        self._data = initial_data or {}
        self.setex_calls: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._data[key] = value
        self.setex_calls.append((key, ttl, value))


def test_build_es_related_movie_items_prioritizes_and_keeps_all_collection_movies():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        genres=["액션", "드라마"],
        director="크리스토퍼 놀란",
        cast_members=["크리스천 베일", "마이클 케인"],
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        ESSearchMovieItem(
            movie_id="3",
            title="다크 나이트 라이즈",
            title_en=None,
            genres=["액션", "드라마"],
            release_year=2012,
            rating=8.4,
            vote_count=3000,
            poster_path="/rises.jpg",
            trailer_url=None,
            overview="브루스 웨인의 마지막 전투",
            director="크리스토퍼 놀란",
            cast=["크리스천 베일"],
            collection_name="다크 나이트 트릴로지",
            score=8.5,
        ),
        ESSearchMovieItem(
            movie_id="2",
            title="다크 나이트",
            title_en=None,
            genres=["액션", "범죄"],
            release_year=2008,
            rating=9.0,
            vote_count=5000,
            poster_path="/knight.jpg",
            trailer_url=None,
            overview="조커와 맞서는 배트맨",
            director="크리스토퍼 놀란",
            cast=["크리스천 베일", "마이클 케인"],
            collection_name="다크 나이트 트릴로지",
            score=9.5,
        ),
    ]
    candidate_movies = [
        collection_movies[1],
        collection_movies[0],
        ESSearchMovieItem(
            movie_id="4",
            title="인셉션",
            title_en=None,
            genres=["액션", "SF"],
            release_year=2010,
            rating=8.8,
            vote_count=4100,
            poster_path="/inception.jpg",
            trailer_url=None,
            overview="꿈속의 꿈",
            director="크리스토퍼 놀란",
            cast=["마이클 케인"],
            collection_name="놀란 대표작",
            score=8.9,
        ),
    ]

    related_items = service._build_es_related_movie_items(
        source_movie=source_movie,
        collection_movies=collection_movies,
        candidate_movies=candidate_movies,
        limit=1,
    )

    assert [item.movie_id for item in related_items] == ["2", "3"]
    assert related_items[0].relation_reasons == [
        "같은 컬렉션: 다크 나이트 트릴로지",
        "같은 장르: 액션",
        "공통 출연 2명",
    ]
    assert related_items[0].relation_sources == [
        "elasticsearch_collection",
        "elasticsearch_related",
    ]
    assert related_items[1].relation_sources == ["elasticsearch_collection", "elasticsearch_related"]


def test_build_es_related_movie_items_excludes_collection_movies_when_requested():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        genres=["액션", "드라마"],
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        ESSearchMovieItem(
            movie_id="2",
            title="다크 나이트",
            title_en=None,
            genres=["액션", "범죄"],
            release_year=2008,
            rating=9.0,
            vote_count=5000,
            poster_path="/knight.jpg",
            trailer_url=None,
            overview="조커와 맞서는 배트맨",
            director="크리스토퍼 놀란",
            cast=["크리스천 베일"],
            collection_name="다크 나이트 트릴로지",
            score=9.5,
        ),
    ]
    candidate_movies = [
        collection_movies[0],
        ESSearchMovieItem(
            movie_id="4",
            title="인셉션",
            title_en=None,
            genres=["액션", "SF"],
            release_year=2010,
            rating=8.8,
            vote_count=4100,
            poster_path="/inception.jpg",
            trailer_url=None,
            overview="꿈속의 꿈",
            director="크리스토퍼 놀란",
            cast=["마이클 케인"],
            collection_name="놀란 대표작",
            score=8.9,
        ),
    ]

    related_items = service._build_es_related_movie_items(
        source_movie=source_movie,
        collection_movies=collection_movies,
        candidate_movies=candidate_movies,
        limit=5,
        include_collection_movies=False,
    )

    assert [item.movie_id for item in related_items] == ["4"]


def test_build_es_related_movie_items_excludes_same_collection_by_name_without_collection_list():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        genres=["액션", "드라마"],
        collection_name="다크 나이트 트릴로지",
    )
    candidate_movies = [
        ESSearchMovieItem(
            movie_id="2",
            title="다크 나이트",
            title_en=None,
            genres=["액션", "범죄"],
            release_year=2008,
            rating=9.0,
            vote_count=5000,
            poster_path="/knight.jpg",
            trailer_url=None,
            overview="조커와 맞서는 배트맨",
            director="크리스토퍼 놀란",
            cast=["크리스천 베일"],
            collection_name="다크 나이트 트릴로지",
            score=9.5,
        ),
        ESSearchMovieItem(
            movie_id="4",
            title="인셉션",
            title_en=None,
            genres=["액션", "SF"],
            release_year=2010,
            rating=8.8,
            vote_count=4100,
            poster_path="/inception.jpg",
            trailer_url=None,
            overview="꿈속의 꿈",
            director="크리스토퍼 놀란",
            cast=["마이클 케인"],
            collection_name="놀란 대표작",
            score=8.9,
        ),
    ]

    related_items = service._build_es_related_movie_items(
        source_movie=source_movie,
        collection_movies=[],
        candidate_movies=candidate_movies,
        limit=5,
        include_collection_movies=False,
    )

    assert [item.movie_id for item in related_items] == ["4"]


def test_sort_general_es_candidates_prefers_genre_similarity_over_director_only_match():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="기생충",
        genres=["드라마", "스릴러"],
        director="봉준호",
        cast_members=["송강호"],
    )
    same_director_only = ESSearchMovieItem(
        movie_id="2",
        title="설국열차",
        title_en=None,
        genres=["SF"],
        release_year=2013,
        rating=7.1,
        vote_count=2200,
        poster_path=None,
        trailer_url=None,
        overview="열차 계급 사회 이야기",
        director="봉준호",
        cast=[],
        collection_name=None,
        score=9.0,
    )
    shared_genre_candidate = ESSearchMovieItem(
        movie_id="3",
        title="버닝",
        title_en=None,
        genres=["드라마", "스릴러"],
        release_year=2018,
        rating=7.5,
        vote_count=1800,
        poster_path=None,
        trailer_url=None,
        overview="청춘의 불안과 미스터리",
        director="이창동",
        cast=[],
        collection_name=None,
        score=6.5,
    )

    sorted_candidates = service._sort_general_es_candidates(
        candidate_movies=[same_director_only, shared_genre_candidate],
        source_genres=set(source_movie.get_genres_list()),
        source_cast=set(source_movie.get_cast_list()),
        source_director=source_movie.director,
        source_collection="",
    )

    assert [movie.movie_id for movie in sorted_candidates] == ["3", "2"]


def test_build_related_movie_items_prefers_qdrant_vector_similarity_over_director_match():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="기생충",
        genres=["드라마", "스릴러"],
        director="봉준호",
        cast_members=["송강호"],
    )
    vector_movie = MovieDTO(
        movie_id="2",
        title="버닝",
        genres=["드라마"],
        release_year=2018,
        rating=7.5,
        vote_count=1500,
        overview="청춘과 미스터리",
        poster_path="/burning.jpg",
    )
    same_director_movie = MovieDTO(
        movie_id="3",
        title="설국열차",
        genres=["SF"],
        release_year=2013,
        rating=7.1,
        vote_count=2200,
        director="봉준호",
        overview="열차 안 계급 사회",
        poster_path="/snowpiercer.jpg",
    )

    related_items = service._build_related_movie_items(
        source_movie=source_movie,
        collection_movies=[],
        candidate_map={
            "2": RelatedCandidate(
                score=130.0,
                qdrant_vector_similarity=0.92,
                qdrant_vector_rank=0,
                reasons=["줄거리 벡터 유사", "같은 장르: 드라마"],
                sources=["qdrant_plot_vector"],
            ),
            "3": RelatedCandidate(
                score=131.0,
                qdrant_vector_similarity=0.12,
                qdrant_vector_rank=8,
                reasons=["같은 감독: 봉준호"],
                sources=["neo4j_director"],
            ),
        },
        candidate_movies=[vector_movie, same_director_movie],
        limit=2,
    )

    assert [item.movie_id for item in related_items] == ["2", "3"]
    assert related_items[0].relation_reasons[0] == "줄거리 벡터 유사"


def test_build_related_movie_items_excludes_same_collection_by_name_without_collection_lookup():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movie = MovieDTO(
        movie_id="2",
        title="다크 나이트",
        collection_name="다크 나이트 트릴로지",
        poster_path="/knight.jpg",
    )
    non_collection_movie = MovieDTO(
        movie_id="3",
        title="인셉션",
        collection_name="놀란 대표작",
        poster_path="/inception.jpg",
    )

    related_items = service._build_related_movie_items(
        source_movie=source_movie,
        collection_movies=[],
        candidate_map={
            "2": RelatedCandidate(
                score=140.0,
                qdrant_vector_similarity=0.95,
                qdrant_vector_rank=0,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
            "3": RelatedCandidate(
                score=120.0,
                qdrant_vector_similarity=0.82,
                qdrant_vector_rank=1,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
        },
        candidate_movies=[collection_movie, non_collection_movie],
        limit=10,
    )

    assert [item.movie_id for item in related_items] == ["3"]


def test_build_related_movie_items_excludes_movies_without_poster():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="기생충",
        genres=["드라마", "스릴러"],
    )
    collection_without_poster = MovieDTO(
        movie_id="2",
        title="컬렉션 영화",
        collection_name="테스트 컬렉션",
        release_year=2018,
        vote_count=1200,
        poster_path=None,
    )
    candidate_without_poster = MovieDTO(
        movie_id="3",
        title="포스터 없는 후보",
        release_year=2019,
        rating=7.0,
        vote_count=800,
        poster_path=None,
    )
    candidate_with_poster = MovieDTO(
        movie_id="4",
        title="포스터 있는 후보",
        release_year=2020,
        rating=7.5,
        vote_count=1000,
        poster_path="/poster.jpg",
    )

    related_items = service._build_related_movie_items(
        source_movie=source_movie,
        collection_movies=[collection_without_poster],
        candidate_map={
            "3": RelatedCandidate(
                score=130.0,
                qdrant_vector_similarity=0.93,
                qdrant_vector_rank=0,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
            "4": RelatedCandidate(
                score=120.0,
                qdrant_vector_similarity=0.82,
                qdrant_vector_rank=1,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
        },
        candidate_movies=[candidate_without_poster, candidate_with_poster],
        limit=3,
    )

    assert [item.movie_id for item in related_items] == ["4"]


def test_build_related_movie_items_excludes_collection_movies_from_general_related():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movie = MovieDTO(
        movie_id="2",
        title="다크 나이트",
        collection_name="다크 나이트 트릴로지",
        release_year=2008,
        vote_count=5000,
        poster_path="/knight.jpg",
    )
    non_collection_movie = MovieDTO(
        movie_id="3",
        title="인셉션",
        release_year=2010,
        rating=8.8,
        vote_count=4100,
        poster_path="/inception.jpg",
    )

    related_items = service._build_related_movie_items(
        source_movie=source_movie,
        collection_movies=[collection_movie],
        candidate_map={
            "2": RelatedCandidate(
                score=160.0,
                qdrant_vector_similarity=0.98,
                qdrant_vector_rank=0,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
            "3": RelatedCandidate(
                score=120.0,
                qdrant_vector_similarity=0.85,
                qdrant_vector_rank=1,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            ),
        },
        candidate_movies=[collection_movie, non_collection_movie],
        limit=5,
    )

    assert [item.movie_id for item in related_items] == ["3"]


@pytest.mark.asyncio
async def test_get_related_movies_returns_cached_response_first():
    cached_response = RelatedMoviesResponse(
        movies=[
            RelatedMovieItem(
                movie_id="11",
                title="캐시된 영화",
                relation_reasons=["같은 컬렉션: 테스트 컬렉션"],
                relation_sources=["redis_cache"],
            )
        ]
    )
    redis = FakeRedis(
        {
            RelatedMovieService._cache_key("1", 25): cached_response.model_dump_json(),
        }
    )
    service = RelatedMovieService(conn=None, redis_client=redis)

    async def unexpected_find_by_id(_: str):
        raise AssertionError("cache hit 경로에서는 DB를 조회하면 안 됩니다.")

    service._movie_repo.find_by_id = unexpected_find_by_id

    response = await service.get_related_movies("1", limit=25)

    assert [item.movie_id for item in response.movies] == ["11"]
    assert redis.setex_calls == []


@pytest.mark.asyncio
async def test_get_collection_related_movies_uses_es_results_first():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        ESSearchMovieItem(
            movie_id="3",
            title="다크 나이트 라이즈",
            title_en=None,
            genres=[],
            collection_name="다크 나이트 트릴로지",
            release_year=2012,
            rating=None,
            vote_count=3000,
            poster_path="/rises.jpg",
            trailer_url=None,
            overview=None,
        ),
        ESSearchMovieItem(
            movie_id="2",
            title="다크 나이트",
            title_en=None,
            genres=[],
            collection_name="다크 나이트 트릴로지",
            release_year=2008,
            rating=None,
            vote_count=5000,
            poster_path="/knight.jpg",
            trailer_url=None,
            overview=None,
        ),
    ]

    async def find_by_id(_: str) -> MovieDTO:
        return source_movie

    service._movie_repo.find_by_id = find_by_id
    service._movie_repo.find_by_collection_name = AsyncMock(
        side_effect=AssertionError("ES 성공 시 MySQL 폴백은 호출되면 안 됩니다.")
    )
    service._search_es.search_collection_movies = AsyncMock(return_value=collection_movies)

    response = await service.get_collection_related_movies("1")

    assert [item.movie_id for item in response.movies] == ["2", "3"]
    assert response.movies[0].relation_reasons[0] == "같은 컬렉션: 다크 나이트 트릴로지"
    assert response.movies[0].relation_sources == ["collection_priority", "elasticsearch_collection"]


@pytest.mark.asyncio
async def test_get_collection_related_movies_falls_back_to_mysql_when_es_unavailable():
    service = RelatedMovieService(conn=None)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        MovieDTO(
            movie_id="3",
            title="다크 나이트 라이즈",
            collection_name="다크 나이트 트릴로지",
            release_year=2012,
            vote_count=3000,
            poster_path="/rises.jpg",
        ),
        MovieDTO(
            movie_id="2",
            title="다크 나이트",
            collection_name="다크 나이트 트릴로지",
            release_year=2008,
            vote_count=5000,
            poster_path="/knight.jpg",
        ),
    ]

    async def find_by_id(_: str) -> MovieDTO:
        return source_movie

    async def find_by_collection_name(collection_name: str, exclude_movie_id: str | None = None) -> list[MovieDTO]:
        assert collection_name == "다크 나이트 트릴로지"
        assert exclude_movie_id == "1"
        return collection_movies

    service._movie_repo.find_by_id = find_by_id
    service._movie_repo.find_by_collection_name = find_by_collection_name
    service._search_es.search_collection_movies = AsyncMock(return_value=None)

    response = await service.get_collection_related_movies("1")

    assert [item.movie_id for item in response.movies] == ["2", "3"]
    assert response.movies[0].relation_reasons[0] == "같은 컬렉션: 다크 나이트 트릴로지"


@pytest.mark.asyncio
async def test_get_related_movies_caches_final_response_and_skips_neo4j():
    redis = FakeRedis()
    service = RelatedMovieService(conn=None, redis_client=redis)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        MovieDTO(
            movie_id="2",
            title="다크 나이트",
            collection_name="다크 나이트 트릴로지",
            release_year=2008,
            vote_count=5000,
            poster_path="/knight.jpg",
        )
    ]
    qdrant_movie = MovieDTO(
        movie_id="3",
        title="인셉션",
        release_year=2010,
        rating=8.8,
        vote_count=4100,
        overview="꿈속의 꿈",
        poster_path="/inception.jpg",
    )

    async def find_by_id(_: str) -> MovieDTO:
        return source_movie

    async def find_by_collection_name(_: str, exclude_movie_id: str | None = None) -> list[MovieDTO]:
        assert exclude_movie_id == "1"
        return collection_movies

    async def find_by_identifiers(ids: list[str]) -> list[MovieDTO]:
        assert ids == ["3"]
        return [qdrant_movie]

    async def fetch_qdrant_candidates(_: MovieDTO, limit: int) -> dict[str, RelatedCandidate]:
        assert limit >= 20
        return {
            "3": RelatedCandidate(
                score=140.0,
                qdrant_vector_similarity=0.91,
                qdrant_vector_rank=0,
                reasons=["줄거리 벡터 유사"],
                sources=["qdrant_plot_vector"],
            )
        }

    async def unexpected_neo4j(*args, **kwargs):
        raise AssertionError("Neo4j는 실시간 경로에서 호출되면 안 됩니다.")

    async def unexpected_es(*args, **kwargs):
        raise AssertionError("Qdrant 결과가 있으면 ES 폴백은 호출되면 안 됩니다.")

    service._movie_repo.find_by_id = find_by_id
    service._movie_repo.find_by_collection_name = find_by_collection_name
    service._movie_repo.find_by_identifiers = find_by_identifiers
    service._fetch_qdrant_candidates = fetch_qdrant_candidates
    service._fetch_neo4j_candidates = unexpected_neo4j
    service._fetch_es_related_movies = unexpected_es

    response = await service.get_related_movies("1", limit=25)

    assert [item.movie_id for item in response.movies] == ["3"]
    assert response.movies[0].relation_reasons[0] == "줄거리 벡터 유사"
    assert redis.setex_calls
    assert redis.setex_calls[0][0] == RelatedMovieService._cache_key("1", 25)


@pytest.mark.asyncio
async def test_get_related_movies_does_not_cache_collection_only_response_when_qdrant_fails():
    redis = FakeRedis()
    service = RelatedMovieService(conn=None, redis_client=redis)
    source_movie = MovieDTO(
        movie_id="1",
        title="배트맨 비긴즈",
        collection_name="다크 나이트 트릴로지",
    )
    collection_movies = [
        MovieDTO(
            movie_id="2",
            title="다크 나이트",
            collection_name="다크 나이트 트릴로지",
            release_year=2008,
            vote_count=5000,
            poster_path="/knight.jpg",
        )
    ]

    async def find_by_id(_: str) -> MovieDTO:
        return source_movie

    async def find_by_collection_name(_: str, exclude_movie_id: str | None = None) -> list[MovieDTO]:
        assert exclude_movie_id == "1"
        return collection_movies

    async def unexpected_find_by_identifiers(_: list[str]) -> list[MovieDTO]:
        raise AssertionError("Qdrant 실패 시 후보 영화 재조회는 호출되지 않아야 합니다.")

    async def failed_qdrant(*args, **kwargs):
        raise RuntimeError("qdrant unavailable")

    service._movie_repo.find_by_id = find_by_id
    service._movie_repo.find_by_collection_name = find_by_collection_name
    service._movie_repo.find_by_identifiers = unexpected_find_by_identifiers
    service._fetch_qdrant_candidates = failed_qdrant
    service._fetch_es_related_movies = AsyncMock(return_value=[])

    response = await service.get_related_movies("1", limit=25)

    assert response.movies == []
    assert redis.setex_calls == []
