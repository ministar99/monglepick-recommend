from datetime import datetime, timezone

from app.v2.model.dto import WorldcupResultDTO, WorldcupSessionDTO


def test_worldcup_result_dto_mysql_bit_bytes_are_normalized_to_bool():
    """aiomysql BIT(1) bytes가 WorldcupResultDTO에서 bool로 정규화됩니다."""
    dto = WorldcupResultDTO(
        worldcup_result_id=1,
        user_id="user_1",
        round_size=16,
        winner_movie_id="movie_1",
        onboarding_completed=b"\x01",
        reward_granted=b"\x00",
        created_at=datetime.now(timezone.utc),
    )

    assert dto.onboarding_completed is True
    assert dto.reward_granted is False


def test_worldcup_session_dto_mysql_bit_bytes_are_normalized_to_bool():
    """aiomysql BIT(1) bytes가 WorldcupSessionDTO에서도 bool로 정규화됩니다."""
    dto = WorldcupSessionDTO(
        session_id=1,
        user_id="user_1",
        source_type="CATEGORY",
        candidate_pool_size=16,
        round_size=16,
        current_round=16,
        status="IN_PROGRESS",
        started_at=datetime.now(timezone.utc),
        reward_granted=b"\x01",
    )

    assert dto.reward_granted is True
