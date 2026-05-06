"""
온보딩(개인화 초기 설정) API 테스트

DDL 기준: movie_id VARCHAR(50) PK, user_id VARCHAR(50) PK
SQLite 인메모리 DB + FakeRedis를 사용합니다.

테스트 대상:
- GET  /api/v1/onboarding/genres: 장르 목록 + 대표 영화
- POST /api/v1/onboarding/genres: 장르 선택 저장
- GET  /api/v1/onboarding/worldcup/categories: 월드컵 카테고리 목록
- POST /api/v1/onboarding/worldcup/options: 월드컵 시작 가능 라운드 계산
- POST /api/v1/onboarding/worldcup/start: 월드컵 대진표 생성
- POST /api/v1/onboarding/worldcup: 월드컵 라운드 결과 제출
- GET  /api/v1/onboarding/worldcup/result: 월드컵 결과 분석
- GET  /api/v1/onboarding/moods: 무드 태그 목록
- POST /api/v1/onboarding/moods: 무드 선택 저장
- GET  /api/v1/onboarding/status: 시작 미션 상태 확인
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import (
    Movie,
    User,
    WorldcupCandidate,
    WorldcupCategory,
    WorldcupMatch,
    WorldcupResult,
    WorldcupSession,
)
from tests.conftest import TEST_USER_ID


# ─────────────────────────────────────────
# 테스트 데이터 삽입 헬퍼
# ─────────────────────────────────────────
async def _insert_test_user(session: AsyncSession) -> User:
    """
    테스트 사용자를 DB에 삽입합니다.

    DDL 기준: user_id VARCHAR(50) PK
    인증 컬럼(password_hash, provider 등)은 DDL에 존재하지만 기본값이 있어 생략 가능합니다.
    """
    user = User(
        user_id=TEST_USER_ID,
        email="test@monglepick.com",
        nickname="테스트유저",
    )
    session.add(user)
    await session.flush()
    return user


async def _insert_movies_for_worldcup(session: AsyncSession) -> list[Movie]:
    """
    월드컵용 영화 데이터를 충분히 삽입합니다 (20개).

    DDL 기준: movie_id VARCHAR(50) PK, release_year INT, genres JSON
    """
    movies = []
    genres_pool = [
        ["액션", "SF"], ["액션", "드라마"], ["액션", "코미디"],
        ["액션", "스릴러"], ["액션", "판타지"], ["액션", "SF"],
        ["액션", "드라마"], ["액션", "코미디"], ["액션", "스릴러"],
        ["액션", "판타지"], ["액션", "SF"], ["액션", "드라마"],
        ["액션", "코미디"], ["액션", "스릴러"], ["액션", "판타지"],
        ["액션", "SF"], ["드라마", "로맨스"], ["코미디", "판타지"],
        ["스릴러", "범죄"], ["SF", "판타지"],
    ]
    titles = [
        "테스트영화A", "테스트영화B", "테스트영화C", "테스트영화D",
        "테스트영화E", "테스트영화F", "테스트영화G", "테스트영화H",
        "테스트영화I", "테스트영화J", "테스트영화K", "테스트영화L",
        "테스트영화M", "테스트영화N", "테스트영화O", "테스트영화P",
        "테스트영화Q", "테스트영화R", "테스트영화S", "테스트영화T",
    ]
    for i in range(20):
        movie = Movie(
            movie_id=str(1000 + i),
            title=titles[i],
            title_en=f"TestMovie{chr(65 + i)}",
            overview=f"테스트 영화 설명 {i + 1}",
            genres=genres_pool[i],
            release_year=2020,
            rating=7.0 + (i % 3) * 0.5,
            vote_count=150,
            # 장르 우선순위가 같을 때 popularity_score 높은 영화가 먼저 선발되는지 검증합니다.
            popularity_score=float(i + 1),
            poster_path=f"/test{i + 1}.jpg",
            director=f"감독{i + 1}",
        )
        movies.append(movie)
        session.add(movie)
    await session.flush()
    return movies


async def _insert_worldcup_category_and_candidates(
    session: AsyncSession,
    movies: list[Movie],
) -> WorldcupCategory:
    """테스트용 월드컵 카테고리와 활성 후보를 생성합니다."""
    category = WorldcupCategory(
        category_code="TEST_ACTION",
        category_name="테스트 액션 월드컵",
        description="테스트용 카테고리",
        display_order=1,
        is_enabled=True,
    )
    session.add(category)
    await session.flush()

    for movie in movies[:16]:
        session.add(
            WorldcupCandidate(
                movie_id=movie.movie_id,
                category_id=category.category_id,
                popularity=10.0,
                is_active=True,
            )
        )
    await session.flush()
    return category


async def _insert_worldcup_genre_master(session: AsyncSession) -> None:
    """커스텀 월드컵 장르 목록 테스트용 genre_master 데이터를 생성합니다."""
    await session.execute(text("DROP TABLE IF EXISTS genre_master"))
    await session.execute(text("""
        CREATE TABLE genre_master (
            genre_id VARCHAR(50) PRIMARY KEY,
            genre_code VARCHAR(50) NOT NULL,
            genre_name VARCHAR(100) NOT NULL,
            contents_count INTEGER NOT NULL,
            created_at DATETIME NULL,
            updated_at DATETIME NULL,
            created_by VARCHAR(50) NULL,
            updated_by VARCHAR(50) NULL
        )
    """))
    await session.execute(text("""
        INSERT INTO genre_master (genre_id, genre_code, genre_name, contents_count)
        VALUES
          ('g1', 'ACTION', '액션', 250),
          ('g2', 'ROMANCE', '로맨스', 101),
          ('g3', 'EROTIC', '에로', 999),
          ('g4', 'DOCU', '다큐멘터리', 80),
          ('g5', 'DIVISION', '반공/분단', 500)
    """))
    await session.flush()


async def _ensure_onboarding_mission_tables(session: AsyncSession) -> None:
    """시작 미션 상태 테스트용 fav_genre / fav_movie 테이블을 생성합니다."""
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS fav_genre (
            fav_genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id VARCHAR(50) NOT NULL,
            genre_id VARCHAR(50) NOT NULL,
            priority INTEGER NULL
        )
    """))
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS fav_movie (
            fav_movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id VARCHAR(50) NOT NULL,
            movie_id VARCHAR(50) NOT NULL,
            priority INTEGER NULL
        )
    """))
    await session.flush()


# =========================================
# 장르 선택 테스트
# =========================================

@pytest.mark.asyncio
async def test_get_genres(client: AsyncClient, async_session: AsyncSession):
    """장르 목록과 대표 영화가 반환됩니다."""
    await _insert_movies_for_worldcup(async_session)

    response = await client.get("/api/v1/onboarding/genres")
    assert response.status_code == 200

    data = response.json()
    assert "genres" in data
    # 최소 1개 이상의 장르가 반환되어야 함
    assert len(data["genres"]) > 0


@pytest.mark.asyncio
async def test_save_genre_selection(client: AsyncClient, async_session: AsyncSession):
    """장르 선택이 올바르게 저장됩니다."""
    await _insert_test_user(async_session)

    response = await client.post(
        "/api/v1/onboarding/genres",
        json={"selected_genres": ["액션", "SF", "드라마"]},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["selected_genres"] == ["액션", "SF", "드라마"]


@pytest.mark.asyncio
async def test_save_genre_selection_min_3(client: AsyncClient, async_session: AsyncSession):
    """3개 미만 장르 선택은 422 에러를 반환합니다."""
    await _insert_test_user(async_session)

    response = await client.post(
        "/api/v1/onboarding/genres",
        json={"selected_genres": ["액션", "SF"]},
    )
    # 최소 3개 검증 (Pydantic min_length=3)
    assert response.status_code == 422


# =========================================
# 이상형 월드컵 테스트
# =========================================

@pytest.mark.asyncio
async def test_get_worldcup_categories(client: AsyncClient, async_session: AsyncSession):
    """월드컵 카테고리 목록과 가능 라운드가 반환됩니다."""
    await _insert_test_user(async_session)
    movies = await _insert_movies_for_worldcup(async_session)
    category = await _insert_worldcup_category_and_candidates(async_session, movies)

    response = await client.get("/api/v1/onboarding/worldcup/categories")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    assert data[0]["categoryId"] == category.category_id
    assert data[0]["candidatePoolSize"] == 16
    assert 16 in data[0]["availableRoundSizes"]
    assert 8 in data[0]["availableRoundSizes"]
    assert data[0]["previewMovieId"] is not None
    assert data[0]["previewPosterUrl"] is not None
    assert data[0]["isReady"] is True


@pytest.mark.asyncio
async def test_get_worldcup_genres(client: AsyncClient, async_session: AsyncSession):
    """커스텀 월드컵 장르 목록은 genre_master 필터 규칙을 반영합니다."""
    await _insert_test_user(async_session)
    await _insert_worldcup_genre_master(async_session)

    response = await client.get("/api/v1/onboarding/worldcup/genres")
    assert response.status_code == 200

    data = response.json()
    names = [item["genreName"] for item in data]
    assert names == ["액션", "로맨스"]
    assert all(item["contentsCount"] > 100 for item in data)


@pytest.mark.asyncio
async def test_get_worldcup_start_options_for_genre(client: AsyncClient, async_session: AsyncSession):
    """장르 기반 월드컵의 후보 수와 가능 라운드가 계산됩니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    response = await client.post(
        "/api/v1/onboarding/worldcup/options",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
        },
    )
    assert response.status_code == 200

    data = response.json()
    assert data["sourceType"] == "GENRE"
    assert data["candidatePoolSize"] >= 16
    assert 16 in data["availableRoundSizes"]


@pytest.mark.asyncio
async def test_generate_worldcup_bracket(client: AsyncClient, async_session: AsyncSession):
    """새 시작 요청으로 월드컵 대진표가 올바르게 생성됩니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    response = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
            "roundSize": 16,
        },
    )
    assert response.status_code == 200

    data = response.json()
    assert data["round_size"] == 16
    assert len(data["matches"]) == 8  # 16강 = 8매치
    assert data["total_rounds"] == 4  # 16→8→4→2 = 4라운드

    # 각 매치에 2개 영화가 있는지 확인 (movie_id는 VARCHAR(50))
    for match in data["matches"]:
        assert "movie_a" in match
        assert "movie_b" in match
        assert match["movie_a"]["movie_id"] != match["movie_b"]["movie_id"]

    session_result = await async_session.execute(
        select(WorldcupSession).where(WorldcupSession.user_id == TEST_USER_ID)
    )
    saved_session = session_result.scalar_one()
    assert saved_session.source_type == "GENRE"
    assert saved_session.current_round == 16
    assert saved_session.status == "IN_PROGRESS"
    assert saved_session.completed_at is None

    match_result = await async_session.execute(
        select(WorldcupMatch)
        .where(
            WorldcupMatch.session_id == saved_session.session_id,
            WorldcupMatch.round_number == 16,
        )
        .order_by(WorldcupMatch.match_order.asc())
    )
    saved_matches = list(match_result.scalars().all())
    assert len(saved_matches) == 8
    assert all(match.winner_movie_id is None for match in saved_matches)


@pytest.mark.asyncio
async def test_generate_worldcup_bracket_prioritizes_more_genre_matches(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """다중 장르 선택 시 더 많은 선택 장르를 만족하는 영화가 먼저 후보에 포함됩니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    response = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션", "SF", "판타지"],
            "roundSize": 8,
        },
    )
    assert response.status_code == 200

    data = response.json()
    candidate_ids = {
        match["movie_a"]["movie_id"] for match in data["matches"]
    } | {
        match["movie_b"]["movie_id"] for match in data["matches"]
    }

    # 2개 장르를 만족하는 영화 8편이 정확히 먼저 선발되어야 합니다.
    expected_priority_ids = {"1000", "1004", "1005", "1009", "1010", "1014", "1015", "1019"}
    assert candidate_ids == expected_priority_ids


@pytest.mark.asyncio
async def test_generate_worldcup_bracket_prioritizes_popular_movies_within_same_genre_match_count(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """장르 일치 수가 같으면 popularity_score가 높은 영화가 먼저 후보에 포함됩니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    response = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
            "roundSize": 8,
        },
    )
    assert response.status_code == 200

    data = response.json()
    candidate_ids = {
        match["movie_a"]["movie_id"] for match in data["matches"]
    } | {
        match["movie_b"]["movie_id"] for match in data["matches"]
    }

    expected_top_popularity_ids = {"1008", "1009", "1010", "1011", "1012", "1013", "1014", "1015"}
    assert candidate_ids == expected_top_popularity_ids


@pytest.mark.asyncio
async def test_start_worldcup_abandons_previous_in_progress_session(
    client: AsyncClient,
    async_session: AsyncSession,
):
    """새 월드컵 시작 시 기존 진행 중 세션은 ABANDONED 처리됩니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    first_resp = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
            "roundSize": 16,
        },
    )
    assert first_resp.status_code == 200

    second_resp = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
            "roundSize": 8,
        },
    )
    assert second_resp.status_code == 200

    session_rows = await async_session.execute(
        select(WorldcupSession)
        .where(WorldcupSession.user_id == TEST_USER_ID)
        .order_by(WorldcupSession.session_id.asc())
    )
    sessions = list(session_rows.scalars().all())
    assert len(sessions) == 2
    assert sessions[0].status == "ABANDONED"
    assert sessions[1].status == "IN_PROGRESS"
    assert sessions[1].current_round == 8


@pytest.mark.asyncio
async def test_worldcup_full_flow(client: AsyncClient, async_session: AsyncSession):
    """월드컵 전체 흐름 (대진표 생성 → 라운드 진행 → 결과 확인)을 테스트합니다."""
    await _insert_test_user(async_session)
    await _insert_movies_for_worldcup(async_session)

    # 1. 새 시작 요청으로 대진표 생성 (16강)
    bracket_resp = await client.post(
        "/api/v1/onboarding/worldcup/start",
        json={
            "sourceType": "GENRE",
            "selectedGenres": ["액션"],
            "roundSize": 16,
        },
    )
    assert bracket_resp.status_code == 200
    bracket = bracket_resp.json()

    # 2. 16강 진행: 각 매치에서 movie_a를 선택 (movie_id는 VARCHAR(50))
    selections_16 = [m["movie_a"]["movie_id"] for m in bracket["matches"]]
    round_resp = await client.post(
        "/api/v1/onboarding/worldcup",
        json={
            "round_size": 16,
            "selections": selections_16,
            "is_final": False,
        },
    )
    assert round_resp.status_code == 200
    round_data = round_resp.json()
    assert round_data["next_round"] == 8

    session_result = await async_session.execute(
        select(WorldcupSession).where(WorldcupSession.user_id == TEST_USER_ID)
    )
    saved_session = session_result.scalar_one()
    assert saved_session.current_round == 8
    assert saved_session.status == "IN_PROGRESS"

    match_result = await async_session.execute(
        select(WorldcupMatch)
        .where(WorldcupMatch.session_id == saved_session.session_id)
        .order_by(WorldcupMatch.round_number.desc(), WorldcupMatch.match_order.asc())
    )
    saved_matches = list(match_result.scalars().all())
    round_16_matches = [match for match in saved_matches if match.round_number == 16]
    round_8_matches = [match for match in saved_matches if match.round_number == 8]
    assert len(round_16_matches) == 8
    assert len(round_8_matches) == 4
    assert all(match.winner_movie_id is not None for match in round_16_matches)

    # 3. 8강 진행
    selections_8 = [m["movie_a"]["movie_id"] for m in round_data["next_matches"]]
    round_resp = await client.post(
        "/api/v1/onboarding/worldcup",
        json={
            "round_size": 8,
            "selections": selections_8,
            "is_final": False,
        },
    )
    assert round_resp.status_code == 200
    round_data = round_resp.json()
    assert round_data["next_round"] == 4

    await async_session.refresh(saved_session)
    assert saved_session.current_round == 4
    assert saved_session.status == "IN_PROGRESS"

    # 4. 4강 진행
    selections_4 = [m["movie_a"]["movie_id"] for m in round_data["next_matches"]]
    round_resp = await client.post(
        "/api/v1/onboarding/worldcup",
        json={
            "round_size": 4,
            "selections": selections_4,
            "is_final": False,
        },
    )
    assert round_resp.status_code == 200
    round_data = round_resp.json()
    assert round_data["next_round"] == 2

    await async_session.refresh(saved_session)
    assert saved_session.current_round == 2
    assert saved_session.status == "IN_PROGRESS"

    # 5. 결승 진행
    final_selection = [round_data["next_matches"][0]["movie_a"]["movie_id"]]
    final_resp = await client.post(
        "/api/v1/onboarding/worldcup",
        json={
            "round_size": 2,
            "selections": final_selection,
            "is_final": True,
        },
    )
    assert final_resp.status_code == 200
    final_data = final_resp.json()
    assert final_data["next_round"] is None  # 월드컵 종료

    await async_session.refresh(saved_session)
    assert saved_session.status == "COMPLETED"
    assert saved_session.winner_movie_id == final_selection[0]
    assert saved_session.completed_at is not None

    final_match_result = await async_session.execute(
        select(WorldcupMatch)
        .where(WorldcupMatch.session_id == saved_session.session_id)
        .order_by(WorldcupMatch.round_number.asc(), WorldcupMatch.match_order.asc())
    )
    final_matches = list(final_match_result.scalars().all())
    assert len(final_matches) == 15
    assert all(match.winner_movie_id is not None for match in final_matches)

    # 6. 결과 확인
    result_resp = await client.get("/api/v1/onboarding/worldcup/result")
    assert result_resp.status_code == 200

    result = result_resp.json()
    assert "winner" in result
    assert "genre_preferences" in result
    assert "top_genres" in result
    assert len(result["top_genres"]) > 0

    result_row = await async_session.execute(
        select(WorldcupResult)
        .where(WorldcupResult.user_id == TEST_USER_ID)
        .order_by(WorldcupResult.created_at.desc())
        .limit(1)
    )
    saved_result = result_row.scalar_one()
    assert saved_result.session_id == saved_session.session_id


# =========================================
# 무드 선택 테스트
# =========================================

@pytest.mark.asyncio
async def test_get_moods(client: AsyncClient):
    """무드 태그 목록이 반환됩니다."""
    response = await client.get("/api/v1/onboarding/moods")
    assert response.status_code == 200

    data = response.json()
    assert "moods" in data
    assert len(data["moods"]) == 14  # 사전 정의된 14개 무드

    # 각 무드에 id, name, emoji가 있는지 확인
    for mood in data["moods"]:
        assert "id" in mood
        assert "name" in mood
        assert "emoji" in mood


@pytest.mark.asyncio
async def test_save_mood_selection(client: AsyncClient, async_session: AsyncSession):
    """무드 선택이 올바르게 저장됩니다."""
    await _insert_test_user(async_session)

    response = await client.post(
        "/api/v1/onboarding/moods",
        json={"selected_moods": ["긴장감있는", "감동적인", "유쾌한"]},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["selected_moods"] == ["긴장감있는", "감동적인", "유쾌한"]


@pytest.mark.asyncio
async def test_save_mood_empty(client: AsyncClient, async_session: AsyncSession):
    """빈 무드 선택은 422 에러를 반환합니다."""
    response = await client.post(
        "/api/v1/onboarding/moods",
        json={"selected_moods": []},
    )
    assert response.status_code == 422


# =========================================
# 시작 미션 상태 테스트
# =========================================

@pytest.mark.asyncio
async def test_onboarding_status_initial(client: AsyncClient, async_session: AsyncSession):
    """초기 상태에서 시작 미션 3개가 모두 미완료입니다."""
    await _insert_test_user(async_session)
    await _ensure_onboarding_mission_tables(async_session)

    response = await client.get("/api/v1/onboarding/status")
    assert response.status_code == 200

    data = response.json()
    assert data["is_completed"] is False
    assert data["completed_mission_count"] == 0
    assert data["worldcup_completed"] is False
    assert data["favorite_genres_completed"] is False
    assert data["favorite_movies_completed"] is False
    assert data["favorite_genre_count"] == 0
    assert data["favorite_movie_count"] == 0


@pytest.mark.asyncio
async def test_onboarding_status_after_favorite_genre(client: AsyncClient, async_session: AsyncSession):
    """선호 장르 저장 후 해당 미션만 완료로 표시됩니다."""
    await _insert_test_user(async_session)
    await _ensure_onboarding_mission_tables(async_session)

    await async_session.execute(
        text(
            "INSERT INTO fav_genre (user_id, genre_id, priority) "
            "VALUES (:user_id, :genre_id, :priority)"
        ),
        {"user_id": TEST_USER_ID, "genre_id": "g1", "priority": 1},
    )
    await async_session.flush()

    response = await client.get("/api/v1/onboarding/status")
    data = response.json()
    assert data["favorite_genres_completed"] is True
    assert data["favorite_genre_count"] == 1
    assert data["worldcup_completed"] is False
    assert data["favorite_movies_completed"] is False
    assert data["favorite_movie_count"] == 0
    assert data["completed_mission_count"] == 1
    assert data["is_completed"] is False


@pytest.mark.asyncio
async def test_onboarding_status_all_completed(client: AsyncClient, async_session: AsyncSession):
    """세 미션 데이터가 모두 있으면 전체 완료로 표시됩니다."""
    await _insert_test_user(async_session)
    await _ensure_onboarding_mission_tables(async_session)

    await async_session.execute(
        text(
            "INSERT INTO fav_genre (user_id, genre_id, priority) "
            "VALUES (:user_id, :genre_id, :priority)"
        ),
        {"user_id": TEST_USER_ID, "genre_id": "g1", "priority": 1},
    )
    await async_session.execute(
        text(
            "INSERT INTO fav_movie (user_id, movie_id, priority) "
            "VALUES (:user_id, :movie_id, :priority)"
        ),
        {"user_id": TEST_USER_ID, "movie_id": "1001", "priority": 1},
    )
    async_session.add(
        WorldcupResult(
            user_id=TEST_USER_ID,
            round_size=16,
            winner_movie_id="1001",
            runner_up_movie_id="1002",
            onboarding_completed=True,
            reward_granted=False,
            total_matches=15,
        )
    )
    await async_session.flush()

    response = await client.get("/api/v1/onboarding/status")
    assert response.status_code == 200

    data = response.json()
    assert data["worldcup_completed"] is True
    assert data["favorite_genres_completed"] is True
    assert data["favorite_movies_completed"] is True
    assert data["favorite_genre_count"] == 1
    assert data["favorite_movie_count"] == 1
    assert data["completed_mission_count"] == 3
    assert data["is_completed"] is True
