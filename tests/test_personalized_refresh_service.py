from datetime import datetime, timedelta, timezone

import pytest

from app.v2.service.personalized_refresh_service import PersonalizedRefreshService


@pytest.mark.asyncio
async def test_personalized_refresh_status_uses_dirty_flag_and_cooldown(fake_redis):
    user_id = "user-dirty"
    limit = 10
    status_key = PersonalizedRefreshService._status_key(user_id=user_id, limit=limit)

    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=limit,
        reason="wishlist",
        redis_client=fake_redis,
    )
    status = await PersonalizedRefreshService.get_status(
        fake_redis,
        user_id=user_id,
        limit=limit,
        has_cache=True,
    )
    assert status["is_dirty"] is True
    assert status["should_refresh"] is True

    await PersonalizedRefreshService._mark_queued(
        fake_redis,
        user_id=user_id,
        limit=limit,
        reason="search_page",
    )
    queued_status = await PersonalizedRefreshService.get_status(
        fake_redis,
        user_id=user_id,
        limit=limit,
        has_cache=True,
    )
    assert queued_status["is_calculating"] is True
    assert queued_status["should_refresh"] is False

    stale_requested_at = (
        datetime.now(timezone.utc)
        - timedelta(seconds=PersonalizedRefreshService.REFRESH_COOLDOWN_SECONDS + 1)
    ).isoformat()
    await fake_redis.hset(
        status_key,
        mapping={
            "state": "ready",
            "dirty": "1",
            "requested_at": stale_requested_at,
        },
    )
    cooled_down_status = await PersonalizedRefreshService.get_status(
        fake_redis,
        user_id=user_id,
        limit=limit,
        has_cache=True,
    )
    assert cooled_down_status["is_dirty"] is True
    assert cooled_down_status["should_refresh"] is True


@pytest.mark.asyncio
async def test_personalized_refresh_ready_clears_dirty_flag(fake_redis):
    user_id = "user-ready"
    limit = 10

    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=limit,
        reason="favorite_movies",
        redis_client=fake_redis,
    )
    await PersonalizedRefreshService._mark_ready(
        fake_redis,
        user_id=user_id,
        limit=limit,
        reason="login",
        computed_at=datetime.now(timezone.utc),
    )

    status = await PersonalizedRefreshService.get_status(
        fake_redis,
        user_id=user_id,
        limit=limit,
        has_cache=True,
    )
    assert status["cache_state"] == "ready"
    assert status["is_dirty"] is False
    assert status["should_refresh"] is False


@pytest.mark.asyncio
async def test_personalized_refresh_ready_preserves_newer_dirty_marker(fake_redis):
    user_id = "user-rerun"
    limit = 10

    await PersonalizedRefreshService._mark_queued(
        fake_redis,
        user_id=user_id,
        limit=limit,
        reason="login",
    )
    await PersonalizedRefreshService.mark_dirty(
        user_id=user_id,
        limit=limit,
        reason="wishlist",
        redis_client=fake_redis,
    )
    await PersonalizedRefreshService._mark_ready(
        fake_redis,
        user_id=user_id,
        limit=limit,
        reason="login",
        computed_at=datetime.now(timezone.utc),
    )

    status = await PersonalizedRefreshService.get_status(
        fake_redis,
        user_id=user_id,
        limit=limit,
        has_cache=True,
    )
    assert status["cache_state"] == "ready"
    assert status["is_dirty"] is True
