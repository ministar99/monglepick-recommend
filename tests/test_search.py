"""
영화 검색 API 테스트

DDL 기준: movie_id VARCHAR(50) PK, release_year INT, genres JSON
SQLite 인메모리 DB + FakeRedis를 사용하여 외부 의존성 없이 테스트합니다.

테스트 대상:
- GET /api/v1/search/movies: 영화 검색 (키워드, 필터, 정렬, 페이지네이션)
- GET /api/v1/search/autocomplete: 자동완성
- GET /api/v1/search/trending: 인기 검색어
- GET /api/v1/search/recent: 최근 검색어
- DELETE /api/v1/search/recent: 최근 검색어 전체 삭제
- DELETE /api/v1/search/recent/{keyword}: 최근 검색어 개별 삭제
"""

import json
from datetime import date
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import Movie, SearchHistory
from app.repository.trending_repository import TrendingRepository
from app.search_elasticsearch import ESSearchMovieItem, ESSearchMoviesResult
from app.service.search_service import SearchService


# ─────────────────────────────────────────
# 테스트 데이터 삽입 헬퍼
# ─────────────────────────────────────────
async def _insert_test_movies(session: AsyncSession) -> list[Movie]:
    """
    테스트용 영화 데이터를 DB에 삽입합니다.

    DDL 기준: movie_id VARCHAR(50) PK, release_year INT, genres JSON
    """
    movies = [
        Movie(
            movie_id="100",
            title="인터스텔라",
            title_en="Interstellar",
            overview="우주 탐험 SF 영화",
            genres=["SF", "드라마"],
            release_year=2014,
            rating=8.6,
            vote_count=150,
            poster_path="/interstellar.jpg",
            director="크리스토퍼 놀란",
            trailer_url="https://youtu.be/zSWdZVtXT7E",
        ),
        Movie(
            movie_id="200",
            title="기생충",
            title_en="Parasite",
            overview="봉준호 감독의 블랙 코미디 스릴러",
            genres=["드라마", "스릴러"],
            release_year=2019,
            rating=8.5,
            vote_count=120,
            poster_path="/parasite.jpg",
            director="봉준호",
        ),
        Movie(
            movie_id="300",
            title="어벤져스: 엔드게임",
            title_en="Avengers: Endgame",
            overview="마블 히어로 액션 영화",
            genres=["액션", "SF"],
            release_year=2019,
            rating=8.4,
            vote_count=95,
            poster_path="/endgame.jpg",
            director="안소니 루소",
        ),
        Movie(
            movie_id="400",
            title="라라랜드",
            title_en="La La Land",
            overview="로맨틱 뮤지컬 영화",
            genres=["로맨스", "뮤지컬"],
            release_year=2016,
            rating=8.0,
            vote_count=80,
            poster_path="/lalaland.jpg",
            director="데이미언 셔젤",
        ),
    ]
    for movie in movies:
        session.add(movie)
    await session.flush()
    return movies


# =========================================
# 영화 검색 테스트
# =========================================

@pytest.mark.asyncio
async def test_search_movies_no_keyword(client: AsyncClient, async_session: AsyncSession):
    """키워드 없이 검색하면 전체 영화를 반환합니다."""
    await _insert_test_movies(async_session)

    response = await client.get("/api/v1/search/movies")
    assert response.status_code == 200

    data = response.json()
    assert "movies" in data
    assert "pagination" in data
    assert data["pagination"]["total"] == 4


@pytest.mark.asyncio
async def test_search_movies_by_title(client: AsyncClient, async_session: AsyncSession):
    """제목 키워드로 검색합니다."""
    await _insert_test_movies(async_session)

    response = await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})
    assert response.status_code == 200

    data = response.json()
    assert len(data["movies"]) == 1
    assert data["movies"][0]["title"] == "인터스텔라"
    assert data["movies"][0]["trailer_url"] == "https://youtu.be/zSWdZVtXT7E"


@pytest.mark.asyncio
async def test_search_movies_does_not_save_history_by_default(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """검색 API는 save_history=true일 때만 최근 검색어를 저장합니다."""
    await _insert_test_movies(async_session)

    response = await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})
    assert response.status_code == 200

    history_result = await async_session.execute(
        select(SearchHistory).where(SearchHistory.keyword == "인터스텔라")
    )
    history_records = list(history_result.scalars())
    assert history_records == []


@pytest.mark.asyncio
async def test_search_genre_options_endpoint_returns_filtered_catalog(client: AsyncClient):
    """검색용 장르 목록은 정제 규칙이 반영된 형태로 반환됩니다."""
    response = await client.get("/api/v1/search/genres")
    assert response.status_code == 200

    data = response.json()
    labels = [item["label"] for item in data["genres"]]

    assert "공포" in labels
    assert "모험" in labels
    assert "청춘/하이틴" in labels
    assert "인물/전기" in labels
    assert "인물" not in labels
    assert "전기" not in labels
    assert "코메디" not in labels
    assert "에로" not in labels
    assert "동성애" not in labels
    assert "반공/분단" not in labels
    assert "계몽" not in labels
    assert all(item["contents_count"] > 20 for item in data["genres"])


@pytest.mark.asyncio
async def test_search_movies_by_selected_genres_without_keyword(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """장르 탐색 검색 + 평점순 정렬은 평점 참여 인원 수 100명 이상 영화만 평점순으로 반환합니다."""
    # PR #29 이후 vote_count 필터는 `sort_by == "rating"` 인 경우에만 적용된다.
    # 기본(관련도순)에서는 vote_count 미달 영화도 포함되므로 테스트 의도(평점순 +
    # vote_count 필터)를 유지하려면 명시적으로 sort_by=rating 을 전달해야 한다.
    await _insert_test_movies(async_session)

    response = await client.get(
        "/api/v1/search/movies",
        params={"genres": "액션,드라마", "sort_by": "rating", "save_history": True},
    )
    assert response.status_code == 200

    data = response.json()
    titles = [movie["title"] for movie in data["movies"]]

    assert "인터스텔라" in titles
    assert "기생충" in titles
    assert "어벤져스: 엔드게임" not in titles
    assert data["pagination"]["total"] == 2
    assert [movie["vote_count"] for movie in data["movies"]] == [150, 120]
    assert [movie["rating"] for movie in data["movies"]] == [8.6, 8.5]

    history_result = await async_session.execute(
        select(SearchHistory).where(SearchHistory.keyword == "액션,드라마")
    )
    history_records = list(history_result.scalars())

    assert len(history_records) == 1
    assert history_records[0].filters["search_mode"] == "genre_discovery"
    assert history_records[0].filters["genres"] == ["액션", "드라마"]


@pytest.mark.asyncio
async def test_search_movies_by_selected_genres_relevance_does_not_require_vote_count_threshold(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """장르 탐색 검색의 관련도순은 평점 참여 인원 수 100명 조건을 적용하지 않습니다."""
    await _insert_test_movies(async_session)

    async_session.add(
        Movie(
            movie_id="450",
            title="저투표 장르 매치",
            title_en="Low Vote Genre Match",
            overview="장르 탐색 관련도순 테스트용 영화",
            genres=["액션", "드라마"],
            release_year=2024,
            rating=7.2,
            vote_count=10,
            poster_path="/low-vote-genre-match.jpg",
            director="테스트 감독 D",
        )
    )
    await async_session.flush()

    response = await client.get(
        "/api/v1/search/movies",
        params={"genres": "액션,드라마", "sort_by": "relevance"},
    )
    assert response.status_code == 200

    data = response.json()
    titles = [movie["title"] for movie in data["movies"]]

    assert "저투표 장르 매치" in titles


@pytest.mark.asyncio
async def test_search_movies_by_selected_genres_release_date_does_not_require_vote_count_threshold(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """장르 탐색 검색의 최신순은 평점 참여 인원 수 100명 조건을 적용하지 않습니다."""
    await _insert_test_movies(async_session)

    async_session.add(
        Movie(
            movie_id="460",
            title="최신 저투표 장르 매치",
            title_en="Latest Low Vote Genre Match",
            overview="장르 탐색 최신순 테스트용 영화",
            genres=["액션", "드라마"],
            release_year=2025,
            rating=6.8,
            vote_count=5,
            poster_path="/latest-low-vote-genre-match.jpg",
            director="테스트 감독 E",
        )
    )
    async_session.add(
        Movie(
            movie_id="461",
            title="평점 높은 과거 장르 매치",
            title_en="Older High Rated Genre Match",
            overview="최신순 정렬 회귀 테스트용 영화",
            genres=["액션", "드라마"],
            release_year=2018,
            rating=9.8,
            vote_count=500,
            poster_path="/older-high-rated-horror.jpg",
            director="테스트 감독 F",
        )
    )
    await async_session.flush()

    response = await client.get(
        "/api/v1/search/movies",
        params={"genres": "액션,드라마", "sort_by": "release_date", "sort_order": "desc"},
    )
    assert response.status_code == 200

    data = response.json()
    titles = [movie["title"] for movie in data["movies"]]

    # 같은 장르 매치 조건이라면 평점이 더 높아도 과거 영화보다 최신 영화가 먼저 와야 합니다.
    assert titles[0] == "최신 저투표 장르 매치"
    assert titles[1] == "평점 높은 과거 장르 매치"


@pytest.mark.asyncio
async def test_search_movies_by_selected_genres_prioritizes_match_count(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """장르 탐색 검색은 선택 장르를 더 많이 만족한 영화가 먼저 노출됩니다."""
    await _insert_test_movies(async_session)

    prioritized_movies = [
        Movie(
            movie_id="500",
            title="장르 풀매치",
            title_en="Genre Full Match",
            overview="선택 장르를 모두 만족하는 영화",
            genres=["액션", "드라마", "애니메이션"],
            release_year=2024,
            rating=7.5,
            vote_count=220,
            poster_path="/genre-full-match.jpg",
            director="테스트 감독 A",
        ),
        Movie(
            movie_id="600",
            title="장르 투매치",
            title_en="Genre Two Match",
            overview="선택 장르 중 두 개를 만족하는 영화",
            genres=["액션", "드라마"],
            release_year=2023,
            rating=9.9,
            vote_count=240,
            poster_path="/genre-two-match.jpg",
            director="테스트 감독 B",
        ),
        Movie(
            movie_id="700",
            title="장르 원매치",
            title_en="Genre One Match",
            overview="선택 장르 중 한 개만 만족하는 영화",
            genres=["애니메이션"],
            release_year=2022,
            rating=10.0,
            vote_count=260,
            poster_path="/genre-one-match.jpg",
            director="테스트 감독 C",
        ),
    ]
    for movie in prioritized_movies:
        async_session.add(movie)
    await async_session.flush()

    response = await client.get(
        "/api/v1/search/movies",
        params={"genres": "액션,드라마,애니메이션", "size": 10},
    )
    assert response.status_code == 200

    data = response.json()
    titles = [movie["title"] for movie in data["movies"]]

    # 선택 장르 3개 일치 > 2개 일치 > 1개 일치 순으로 우선 노출되어야 합니다.
    assert titles[:5] == [
        "장르 풀매치",
        "장르 투매치",
        "장르 원매치",
        "인터스텔라",
        "기생충",
    ]


@pytest.mark.asyncio
async def test_search_service_uses_elasticsearch_for_genre_discovery(async_session: AsyncSession):
    """장르만 선택한 검색도 ES를 우선 사용합니다."""
    await _insert_test_movies(async_session)
    service = SearchService(async_session)
    service._search_es.search_movies = AsyncMock(return_value=ESSearchMoviesResult(
        movies=[
            ESSearchMovieItem(
                movie_id="es-genre-1",
                title="장르 ES 결과",
                title_en="Genre ES Result",
                genres=["액션", "드라마"],
                release_year=2026,
                rating=8.7,
                vote_count=250,
                poster_path="/genre-es-result.jpg",
                trailer_url=None,
                overview="장르 탐색 ES 결과 테스트",
            )
        ],
        total=1,
        did_you_mean=None,
        related_queries=[],
    ))
    service._movie_repo.search = AsyncMock(side_effect=AssertionError("ES 성공 시 MySQL 폴백이 호출되면 안 됩니다."))

    result = await service.search_movies(
        genres=["액션", "드라마"],
        sort_by="rating",
        sort_order="desc",
    )

    assert result.search_source == "elasticsearch"
    assert [movie.title for movie in result.movies] == ["장르 ES 결과"]
    service._search_es.search_movies.assert_awaited_once()
    kwargs = service._search_es.search_movies.await_args.kwargs
    assert kwargs["keyword"] is None
    assert kwargs["genres"] == ["액션", "드라마"]
    assert kwargs["genre_match_groups"] == [["액션"], ["드라마"]]


@pytest.mark.asyncio
async def test_search_service_falls_back_to_mysql_when_genre_discovery_es_returns_none(
    async_session: AsyncSession,
):
    """장르 탐색 ES가 비어 있으면 기존 MySQL 검색으로 폴백합니다."""
    await _insert_test_movies(async_session)
    service = SearchService(async_session)
    service._search_es.search_movies = AsyncMock(return_value=None)

    result = await service.search_movies(
        genres=["액션", "드라마"],
        sort_by="relevance",
        sort_order="desc",
    )

    assert result.search_source == "mysql"
    assert result.pagination.total == 3
    assert {movie.title for movie in result.movies} == {
        "인터스텔라",
        "기생충",
        "어벤져스: 엔드게임",
    }


@pytest.mark.asyncio
async def test_search_movies_excludes_adult_certification_and_ero_genre(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """검색 결과에서는 청소년관람불가/에로 영화가 공통 제외됩니다."""
    await _insert_test_movies(async_session)

    adult_movies = [
        Movie(
            movie_id="800",
            title="청불 영화",
            title_en="Adults Only Movie",
            overview="청소년 관람 불가 테스트",
            genres=["스릴러"],
            release_year=2020,
            rating=8.1,
            vote_count=140,
            certification="청소년 관람 불가",
            poster_path="/adult-only.jpg",
            director="테스트 감독 F",
        ),
        Movie(
            movie_id="810",
            title="청불 표기 영화",
            title_en="Adults Restricted Movie",
            overview="19세관람가 테스트",
            genres=["드라마"],
            release_year=2021,
            rating=7.9,
            vote_count=130,
            certification="19세관람가(청소년관람불가)",
            poster_path="/adult-restricted.jpg",
            director="테스트 감독 G",
        ),
        Movie(
            movie_id="820",
            title="에로 영화",
            title_en="Erotic Movie",
            overview="에로 장르 테스트",
            genres=["에로", "드라마"],
            release_year=2022,
            rating=9.0,
            vote_count=160,
            poster_path="/erotic-movie.jpg",
            director="테스트 감독 H",
        ),
    ]
    for movie in adult_movies:
        async_session.add(movie)
    await async_session.flush()

    response = await client.get("/api/v1/search/movies", params={"sort_by": "relevance"})
    assert response.status_code == 200

    data = response.json()
    titles = [movie["title"] for movie in data["movies"]]

    assert "청불 영화" not in titles
    assert "청불 표기 영화" not in titles
    assert "에로 영화" not in titles

@pytest.mark.asyncio
async def test_search_movies_all_includes_director_and_actor(
    client: AsyncClient, async_session: AsyncSession
):
    """all 검색은 제목/감독/배우를 모두 포함합니다."""
    await _insert_test_movies(async_session)

    # 감독 이름으로 all 검색
    director_response = await client.get(
        "/api/v1/search/movies",
        params={"q": "봉준호", "search_type": "all"},
    )
    assert director_response.status_code == 200
    director_data = director_response.json()
    assert any(movie["title"] == "기생충" for movie in director_data["movies"])

    # 배우 이름으로 all 검색 (cast_members JSON LIKE 경로)
    movie = await async_session.get(Movie, "100")
    movie.cast_members = ["매튜 맥커너히", "앤 해서웨이"]
    await async_session.flush()

    actor_response = await client.get(
        "/api/v1/search/movies",
        params={"q": "매튜 맥커너히", "search_type": "all"},
    )
    assert actor_response.status_code == 200
    actor_data = actor_response.json()
    assert any(movie["title"] == "인터스텔라" for movie in actor_data["movies"])


@pytest.mark.asyncio
async def test_search_movies_sorting(client: AsyncClient, async_session: AsyncSession):
    """평점 내림차순 정렬을 확인합니다."""
    await _insert_test_movies(async_session)

    response = await client.get(
        "/api/v1/search/movies",
        params={"sort_by": "rating", "sort_order": "desc"},
    )
    assert response.status_code == 200

    data = response.json()
    ratings = [m["rating"] for m in data["movies"]]
    # 내림차순 확인 (None 제외)
    valid_ratings = [r for r in ratings if r is not None]
    assert valid_ratings == sorted(valid_ratings, reverse=True)


@pytest.mark.asyncio
async def test_search_movies_pagination(client: AsyncClient, async_session: AsyncSession):
    """페이지네이션이 올바르게 동작합니다."""
    await _insert_test_movies(async_session)

    # 페이지 크기 2로 첫 페이지 조회
    response = await client.get(
        "/api/v1/search/movies",
        params={"page": 1, "size": 2},
    )
    assert response.status_code == 200

    data = response.json()
    assert len(data["movies"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["size"] == 2
    assert data["pagination"]["total"] == 4
    assert data["pagination"]["total_pages"] == 2


@pytest.mark.asyncio
async def test_search_movies_rating_filter(client: AsyncClient, async_session: AsyncSession):
    """평점 범위 필터가 올바르게 동작합니다."""
    await _insert_test_movies(async_session)

    response = await client.get(
        "/api/v1/search/movies",
        params={"rating_min": 8.5},
    )
    assert response.status_code == 200

    data = response.json()
    # 평점 8.5 이상인 영화만 반환
    for movie in data["movies"]:
        assert movie["rating"] >= 8.5


@pytest.mark.asyncio
async def test_search_movies_director_with_genre_filter(
    client: AsyncClient, async_session: AsyncSession
):
    """감독 검색과 장르 필터를 함께 적용할 수 있습니다."""
    await _insert_test_movies(async_session)

    response = await client.get(
        "/api/v1/search/movies",
        params={
            "q": "데이미언 셔젤",
            "search_type": "director",
            "genre": "로맨스",
        },
    )
    assert response.status_code == 200

    data = response.json()
    assert data["pagination"]["total"] == 1
    assert data["movies"][0]["title"] == "라라랜드"


# =========================================
# 자동완성 테스트
# =========================================

@pytest.mark.asyncio
async def test_autocomplete(client: AsyncClient, async_session: AsyncSession):
    """자동완성이 올바르게 동작합니다."""
    await _insert_test_movies(async_session)

    response = await client.get(
        "/api/v1/search/autocomplete",
        params={"q": "인터"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "suggestions" in data
    assert "인터스텔라" in data["suggestions"]


@pytest.mark.asyncio
async def test_autocomplete_empty_query(client: AsyncClient):
    """빈 검색어는 400 에러를 반환합니다."""
    response = await client.get(
        "/api/v1/search/autocomplete",
        params={"q": ""},
    )
    # 최소 1글자 제한 (min_length=1)
    assert response.status_code == 422


# =========================================
# 인기 검색어 테스트
# =========================================

@pytest.mark.asyncio
async def test_trending_empty(client: AsyncClient):
    """인기 검색어가 없으면 빈 리스트를 반환합니다."""
    response = await client.get("/api/v1/search/trending")
    assert response.status_code == 200

    data = response.json()
    assert data["keywords"] == []


@pytest.mark.asyncio
async def test_trending_after_search(client: AsyncClient, async_session: AsyncSession, fake_redis):
    """검색 후 인기 검색어에 반영됩니다."""
    await _insert_test_movies(async_session)

    # "인터스텔라" 3번 검색
    for _ in range(3):
        await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})

    # "기생충" 1번 검색
    await client.get("/api/v1/search/movies", params={"q": "기생충"})

    # 인기 검색어 조회
    response = await client.get("/api/v1/search/trending")
    assert response.status_code == 200

    data = response.json()
    keywords = data["keywords"]
    assert len(keywords) >= 1
    # "인터스텔라"가 1위여야 함
    assert keywords[0]["keyword"] == "인터스텔라"
    assert keywords[0]["search_count"] == 3


@pytest.mark.asyncio
async def test_trending_increment_failure_does_not_poison_outer_transaction(
    async_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
):
    """인기 검색어 백업이 실패해도 같은 트랜잭션의 다른 변경사항은 커밋 가능해야 합니다."""
    async_session.add(
        SearchHistory(
            user_id="test-user",
            keyword="인터스텔라",
            result_count=1,
            filters={"search_type": "title"},
        )
    )
    await async_session.flush()

    repo = TrendingRepository(async_session)
    original_execute = async_session.execute
    failed_once = False

    async def flaky_execute(statement, *args, **kwargs):
        nonlocal failed_once
        statement_sql = str(statement)
        if not failed_once and "INSERT INTO trending_keywords" in statement_sql:
            failed_once = True
            raise IntegrityError(statement_sql, {}, Exception("duplicate entry"))
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(async_session, "execute", flaky_execute)

    with pytest.raises(IntegrityError):
        await repo.increment("아이언맨")

    await async_session.commit()

    result = await async_session.execute(
        select(SearchHistory).where(SearchHistory.user_id == "test-user")
    )
    histories = list(result.scalars())
    assert len(histories) == 1
    assert histories[0].keyword == "인터스텔라"


@pytest.mark.asyncio
async def test_movie_detail_normalizes_date_typed_kobis_open_dt(async_session: AsyncSession):
    """상세 응답은 date 타입의 kobis_open_dt도 안전하게 문자열로 정규화합니다."""
    service = SearchService(async_session)
    movie = Movie(
        movie_id="711",
        title="테스트 영화",
        title_en="Test Movie",
        release_year=2026,
        kobis_open_dt=date(2026, 4, 13),
    )

    detail = service._to_movie_detail(movie)

    assert detail.kobis_open_dt == "20260413"
    assert detail.release_date == "2026-04-13"


def test_search_history_primary_key_column_matches_backend_schema():
    """검색 이력 ORM은 backend 실DB 컬럼명 search_history_id를 사용해야 합니다."""
    primary_keys = list(SearchHistory.__table__.primary_key.columns)

    assert len(primary_keys) == 1
    assert primary_keys[0].name == "search_history_id"


# =========================================
# 최근 검색어 테스트
# =========================================

@pytest.mark.asyncio
async def test_recent_searches(client: AsyncClient, async_session: AsyncSession):
    """최근 검색어가 올바르게 저장/조회됩니다."""
    await _insert_test_movies(async_session)

    # 검색 실행 (이력 자동 저장)
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라", "save_history": True})
    await client.get("/api/v1/search/movies", params={"q": "기생충", "save_history": True})

    # 최근 검색어 조회
    response = await client.get("/api/v1/search/recent")
    assert response.status_code == 200

    data = response.json()
    keywords = [s["keyword"] for s in data["searches"]]
    assert "인터스텔라" in keywords
    assert "기생충" in keywords


@pytest.mark.asyncio
async def test_recent_searches_deduplicate_same_keyword(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """같은 키워드를 여러 번 검색해도 최근 검색어는 한 번만 노출됩니다."""
    await _insert_test_movies(async_session)

    await client.get("/api/v1/search/movies", params={"q": "인터스텔라", "save_history": True})
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라", "save_history": True})

    response = await client.get("/api/v1/search/recent")
    assert response.status_code == 200

    data = response.json()
    keywords = [s["keyword"] for s in data["searches"]]
    assert keywords.count("인터스텔라") == 1

    result = await async_session.execute(
        select(SearchHistory).where(SearchHistory.keyword == "인터스텔라")
    )
    records = list(result.scalars())
    assert len(records) == 2


@pytest.mark.asyncio
async def test_search_history_is_not_saved_for_pagination_requests(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """무한 스크롤용 2페이지 이후 요청은 검색 히스토리에 저장하지 않습니다."""
    await _insert_test_movies(async_session)

    extra_movies = [
        Movie(
            movie_id="500",
            title="테스트 시리즈 1",
            title_en="Test Series 1",
            overview="페이지네이션 테스트용 영화 1",
            genres=["드라마"],
            release_year=2020,
            rating=7.1,
            poster_path="/test-series-1.jpg",
            director="테스트 감독",
        ),
        Movie(
            movie_id="600",
            title="테스트 시리즈 2",
            title_en="Test Series 2",
            overview="페이지네이션 테스트용 영화 2",
            genres=["드라마"],
            release_year=2021,
            rating=7.2,
            poster_path="/test-series-2.jpg",
            director="테스트 감독",
        ),
    ]
    for movie in extra_movies:
        async_session.add(movie)
    await async_session.flush()

    first_response = await client.get(
        "/api/v1/search/movies",
        params={"q": "테스트", "page": 1, "size": 1, "save_history": True},
    )
    second_response = await client.get(
        "/api/v1/search/movies",
        params={"q": "테스트", "page": 2, "size": 1, "save_history": True},
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200

    result = await async_session.execute(
        select(SearchHistory).where(SearchHistory.keyword == "테스트")
    )
    records = list(result.scalars())

    assert len(records) == 1
    assert records[0].filters["page"] == 1


@pytest.mark.asyncio
async def test_recent_searches_limit_10_with_deduplication(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """최근 검색어는 중복 제거 후 최대 10개까지만 노출됩니다."""
    await _insert_test_movies(async_session)

    for idx in range(35):
        keyword = f"테스트키워드-{idx}"
        await client.get("/api/v1/search/movies", params={"q": keyword, "save_history": True})

    # 가장 최신 키워드를 한 번 더 검색해도 결과 목록에는 중복 없이 1회만 노출돼야 함
    await client.get("/api/v1/search/movies", params={"q": "테스트키워드-34", "save_history": True})

    response = await client.get("/api/v1/search/recent")
    assert response.status_code == 200

    data = response.json()
    searches = data["searches"]
    keywords = [item["keyword"] for item in searches]

    assert len(searches) == 10
    assert len(set(keywords)) == 10
    assert keywords[0] == "테스트키워드-34"
    assert "테스트키워드-25" in keywords
    assert "테스트키워드-24" not in keywords
    assert data["pagination"]["offset"] == 0
    assert data["pagination"]["limit"] == 10
    assert data["pagination"]["has_more"] is True
    assert data["pagination"]["next_offset"] == 10


@pytest.mark.asyncio
async def test_recent_searches_support_offset_pagination_without_duplicates(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """최근 검색어는 offset 기반으로 더 오래된 고유 키워드를 이어서 조회할 수 있습니다."""
    await _insert_test_movies(async_session)

    for idx in range(65):
        keyword = f"페이지키워드-{idx}"
        await client.get("/api/v1/search/movies", params={"q": keyword, "save_history": True})

    # 중복 검색이 있어도 페이지 간 목록에는 같은 키워드가 다시 나오지 않아야 함
    await client.get("/api/v1/search/movies", params={"q": "페이지키워드-64", "save_history": True})
    await client.get("/api/v1/search/movies", params={"q": "페이지키워드-40", "save_history": True})

    first_response = await client.get("/api/v1/search/recent", params={"offset": 0, "limit": 10})
    second_response = await client.get("/api/v1/search/recent", params={"offset": 10, "limit": 10})
    third_response = await client.get("/api/v1/search/recent", params={"offset": 20, "limit": 10})

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert third_response.status_code == 200

    first_data = first_response.json()
    second_data = second_response.json()
    third_data = third_response.json()

    first_keywords = [item["keyword"] for item in first_data["searches"]]
    second_keywords = [item["keyword"] for item in second_data["searches"]]
    third_keywords = [item["keyword"] for item in third_data["searches"]]

    assert len(first_keywords) == 10
    assert len(second_keywords) == 10
    assert len(third_keywords) == 10
    assert set(first_keywords).isdisjoint(second_keywords)
    assert set(first_keywords).isdisjoint(third_keywords)
    assert set(second_keywords).isdisjoint(third_keywords)

    assert first_data["pagination"]["has_more"] is True
    assert first_data["pagination"]["next_offset"] == 10
    assert second_data["pagination"]["has_more"] is True
    assert second_data["pagination"]["next_offset"] == 20
    assert third_data["pagination"]["has_more"] is True
    assert third_data["pagination"]["next_offset"] == 30


@pytest.mark.asyncio
async def test_recent_searches_ignores_legacy_non_dict_filters(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """과거 잘못 저장된 filters 값이 있어도 최근 검색 조회가 500 없이 동작해야 합니다."""
    async_session.add(
        SearchHistory(
            user_id="test_user_1",
            keyword="레거시검색",
            result_count=1,
            filters="legacy-string-filter",
        )
    )
    await async_session.flush()

    response = await client.get("/api/v1/search/recent")
    assert response.status_code == 200

    data = response.json()
    assert data["searches"][0]["keyword"] == "레거시검색"
    assert data["searches"][0]["filters"] is None


@pytest.mark.asyncio
async def test_log_search_click_inserts_per_click(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """검색 결과 클릭은 클릭 횟수만큼 개별 row를 저장합니다."""
    await _insert_test_movies(async_session)

    payload = {
        "keyword": "인터스텔라",
        "clicked_movie_id": "100",
        "result_count": 1,
        "filters": {"search_type": "title", "genre": None, "sort": "relevance"},
    }

    for _ in range(3):
        response = await client.post("/api/v1/search/click", json=payload)
        assert response.status_code == 200
        assert response.json()["saved"] is True

    result = await async_session.execute(
        select(SearchHistory).where(
            SearchHistory.keyword == "인터스텔라",
            SearchHistory.clicked_movie_id == "100",
        )
    )
    records = list(result.scalars())

    assert len(records) == 3
    assert all(record.result_count == 1 for record in records)
    assert all(record.filters["search_type"] == "title" for record in records)


@pytest.mark.asyncio
async def test_delete_recent_keyword(client: AsyncClient, async_session: AsyncSession):
    """개별 검색어 삭제가 올바르게 동작합니다."""
    await _insert_test_movies(async_session)

    # 검색 실행
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라", "save_history": True})

    # 삭제
    response = await client.delete("/api/v1/search/recent/인터스텔라")
    assert response.status_code == 200

    # 삭제 확인
    response = await client.get("/api/v1/search/recent")
    data = response.json()
    keywords = [s["keyword"] for s in data["searches"]]
    assert "인터스텔라" not in keywords


@pytest.mark.asyncio
async def test_delete_all_recent(client: AsyncClient, async_session: AsyncSession):
    """전체 검색어 삭제가 올바르게 동작합니다."""
    await _insert_test_movies(async_session)

    # 검색 실행
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라", "save_history": True})
    await client.get("/api/v1/search/movies", params={"q": "기생충", "save_history": True})

    # 전체 삭제
    response = await client.delete("/api/v1/search/recent")
    assert response.status_code == 200

    # 삭제 확인
    response = await client.get("/api/v1/search/recent")
    data = response.json()
    assert len(data["searches"]) == 0


# =========================================
# 헬스체크 테스트
# =========================================

@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    """헬스체크가 정상 응답합니다."""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "monglepick-recommend"
