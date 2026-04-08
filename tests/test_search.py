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

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import Movie, SearchHistory


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

    # 배우 이름으로 all 검색 (cast JSON LIKE 경로)
    movie = await async_session.get(Movie, "100")
    movie.cast = ["매튜 맥커너히", "앤 해서웨이"]
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


# =========================================
# 최근 검색어 테스트
# =========================================

@pytest.mark.asyncio
async def test_recent_searches(client: AsyncClient, async_session: AsyncSession):
    """최근 검색어가 올바르게 저장/조회됩니다."""
    await _insert_test_movies(async_session)

    # 검색 실행 (이력 자동 저장)
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})
    await client.get("/api/v1/search/movies", params={"q": "기생충"})

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

    await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})

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
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})

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
    await client.get("/api/v1/search/movies", params={"q": "인터스텔라"})
    await client.get("/api/v1/search/movies", params={"q": "기생충"})

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
