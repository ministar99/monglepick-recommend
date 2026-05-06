"""
개인화 TOP picks 백그라운드 재계산 오케스트레이션 서비스.

로그인 직후/취향 변경 직후에 계산 요청만 빠르게 받아 Redis 상태 키를 갱신하고,
실제 추천 계산은 별도 비동기 작업으로 수행한다.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging

import redis.asyncio as aioredis
from fastapi import BackgroundTasks

from app.core.redis import get_redis
from app.v2.core.database import get_pool
from app.v2.service.personalized_search_service import PersonalizedSearchService

logger = logging.getLogger(__name__)


class PersonalizedRefreshService:
    """개인화 TOP picks 백그라운드 계산 스케줄러."""

    STATUS_KEY_PREFIX = "search:personalized_top_picks:status"
    STATUS_VERSION = "v2"
    STATUS_TTL_SECONDS = 3600
    REFRESH_COOLDOWN_SECONDS = 300

    _inflight_keys: set[str] = set()
    _pending_rerun_keys: set[str] = set()

    @classmethod
    async def mark_dirty(
        cls,
        *,
        user_id: str,
        limit: int,
        reason: str,
        redis_client: aioredis.Redis | None = None,
    ) -> None:
        """개인화 캐시 재계산 필요 상태만 기록합니다."""
        await cls._write_status(
            redis_client,
            user_id=user_id,
            limit=limit,
            mapping={
                "dirty": "1",
                "dirty_reason": reason,
                "dirty_at": cls._utcnow_iso(),
            },
        )

    @classmethod
    async def enqueue_refresh(
        cls,
        *,
        user_id: str,
        limit: int,
        reason: str,
        background_tasks: BackgroundTasks | None = None,
        redis_client: aioredis.Redis | None = None,
    ) -> bool:
        """개인화 재계산을 큐에 넣고 상태 키를 queued로 갱신합니다."""
        task_key = cls._task_key(user_id=user_id, limit=limit)
        if task_key in cls._inflight_keys:
            cls._pending_rerun_keys.add(task_key)

        await cls._mark_queued(
            redis_client,
            user_id=user_id,
            limit=limit,
            reason=reason,
        )

        if background_tasks is not None:
            background_tasks.add_task(
                cls.run_refresh,
                user_id=user_id,
                limit=limit,
                reason=reason,
            )
            return True

        asyncio.create_task(
            cls.run_refresh(
                user_id=user_id,
                limit=limit,
                reason=reason,
            )
        )
        return True

    @classmethod
    async def run_refresh(
        cls,
        *,
        user_id: str,
        limit: int,
        reason: str,
    ) -> None:
        """백그라운드에서 개인화 TOP picks를 계산해 캐시를 교체합니다."""
        task_key = cls._task_key(user_id=user_id, limit=limit)
        if task_key in cls._inflight_keys:
            cls._pending_rerun_keys.add(task_key)
            return

        cls._inflight_keys.add(task_key)
        try:
            while True:
                cls._pending_rerun_keys.discard(task_key)
                redis_client = await cls._get_redis_optional()

                await cls._mark_running(
                    redis_client,
                    user_id=user_id,
                    limit=limit,
                    reason=reason,
                )
                computed_at = await cls._compute_and_store(
                    redis_client=redis_client,
                    user_id=user_id,
                    limit=limit,
                )
                await cls._mark_ready(
                    redis_client,
                    user_id=user_id,
                    limit=limit,
                    reason=reason,
                    computed_at=computed_at,
                )

                if task_key not in cls._pending_rerun_keys:
                    break
        except Exception as exc:
            logger.exception(
                "personalized_top_picks_refresh_failed user_id=%s limit=%s error=%s",
                user_id,
                limit,
                exc,
            )
            redis_client = await cls._get_redis_optional()
            await cls._mark_failed(
                redis_client,
                user_id=user_id,
                limit=limit,
                reason=reason,
                error_message=str(exc),
            )
        finally:
            cls._inflight_keys.discard(task_key)

    @classmethod
    async def get_status(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        has_cache: bool,
    ) -> dict[str, object]:
        """현재 개인화 캐시 상태를 조회합니다."""
        default_state = "ready" if has_cache else "empty"
        status = {
            "cache_state": default_state,
            "is_calculating": False,
            "is_dirty": False,
            "should_refresh": False,
            "last_computed_at": None,
        }
        if redis_client is None:
            return status

        try:
            payload = await redis_client.hgetall(cls._status_key(user_id=user_id, limit=limit))
        except Exception as exc:
            logger.warning(
                "personalized_top_picks_status_read_error user_id=%s limit=%s error=%s",
                user_id,
                limit,
                exc,
            )
            return status

        if not payload:
            return status

        raw_state = str(payload.get("state") or "").strip().lower()
        if raw_state in {"queued", "running", "ready", "failed"}:
            status["cache_state"] = raw_state
            status["is_calculating"] = raw_state in {"queued", "running"}

        computed_at = cls._parse_datetime(payload.get("last_computed_at"))
        if computed_at is not None:
            status["last_computed_at"] = computed_at

        is_dirty = cls._parse_bool(payload.get("dirty"))
        status["is_dirty"] = is_dirty

        requested_at = cls._parse_datetime(payload.get("requested_at"))
        cooldown_elapsed = (
            requested_at is None
            or (datetime.now(timezone.utc) - requested_at).total_seconds() >= cls.REFRESH_COOLDOWN_SECONDS
        )
        status["should_refresh"] = (
            has_cache
            and is_dirty
            and not status["is_calculating"]
            and cooldown_elapsed
        )

        if not has_cache and status["cache_state"] == "ready":
            status["cache_state"] = "empty"
            status["is_calculating"] = False

        return status

    @classmethod
    async def _compute_and_store(
        cls,
        *,
        redis_client: aioredis.Redis | None,
        user_id: str,
        limit: int,
    ) -> datetime:
        """별도 커넥션으로 추천 계산을 수행하고 캐시에 저장합니다."""
        pool = await get_pool()
        async with pool.acquire() as conn:
            try:
                service = PersonalizedSearchService(conn, redis_client)
                await service.refresh_top_picks(user_id=user_id, limit=limit)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return datetime.now(timezone.utc)

    @classmethod
    async def _mark_queued(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        reason: str,
    ) -> None:
        """상태 키를 queued로 갱신합니다."""
        await cls._write_status(
            redis_client,
            user_id=user_id,
            limit=limit,
            mapping={
                "state": "queued",
                "reason": reason,
                "requested_at": cls._utcnow_iso(),
                "dirty": "1",
            },
        )

    @classmethod
    async def _mark_running(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        reason: str,
    ) -> None:
        """상태 키를 running으로 갱신합니다."""
        await cls._write_status(
            redis_client,
            user_id=user_id,
            limit=limit,
            mapping={
                "state": "running",
                "reason": reason,
                "started_at": cls._utcnow_iso(),
                "dirty": "1",
            },
        )

    @classmethod
    async def _mark_ready(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        reason: str,
        computed_at: datetime,
    ) -> None:
        """상태 키를 ready로 갱신합니다."""
        has_newer_dirty_marker = await cls._has_newer_dirty_marker(
            redis_client,
            user_id=user_id,
            limit=limit,
        )
        dirty_mapping = (
            {
                "dirty": "1",
            }
            if has_newer_dirty_marker
            else {
                "dirty": "0",
                "dirty_reason": "",
                "dirty_at": "",
            }
        )
        await cls._write_status(
            redis_client,
            user_id=user_id,
            limit=limit,
            mapping={
                "state": "ready",
                "reason": reason,
                "last_computed_at": computed_at.isoformat(),
                "last_error": "",
                **dirty_mapping,
            },
        )

    @classmethod
    async def _mark_failed(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        reason: str,
        error_message: str,
    ) -> None:
        """상태 키를 failed로 갱신합니다."""
        await cls._write_status(
            redis_client,
            user_id=user_id,
            limit=limit,
            mapping={
                "state": "failed",
                "reason": reason,
                "dirty": "1",
                "last_error": error_message[:500],
            },
        )

    @classmethod
    async def _write_status(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
        mapping: dict[str, str],
    ) -> None:
        """상태 키를 Redis hash로 best-effort 저장합니다."""
        if redis_client is None:
            return

        try:
            status_key = cls._status_key(user_id=user_id, limit=limit)
            await redis_client.hset(status_key, mapping=mapping)
            await redis_client.expire(status_key, cls.STATUS_TTL_SECONDS)
        except Exception as exc:
            logger.warning(
                "personalized_top_picks_status_write_error user_id=%s limit=%s error=%s",
                user_id,
                limit,
                exc,
            )

    @classmethod
    async def _get_redis_optional(cls) -> aioredis.Redis | None:
        """공용 Redis 클라이언트를 best-effort로 조회합니다."""
        try:
            return await get_redis()
        except Exception:
            return None

    @classmethod
    async def _has_newer_dirty_marker(
        cls,
        redis_client: aioredis.Redis | None,
        *,
        user_id: str,
        limit: int,
    ) -> bool:
        if redis_client is None:
            return False

        try:
            payload = await redis_client.hgetall(cls._status_key(user_id=user_id, limit=limit))
        except Exception:
            return False

        if not cls._parse_bool(payload.get("dirty")):
            return False

        dirty_at = cls._parse_datetime(payload.get("dirty_at"))
        requested_at = cls._parse_datetime(payload.get("requested_at"))
        if dirty_at is None or requested_at is None:
            return False
        return dirty_at > requested_at

    @classmethod
    def _status_key(cls, *, user_id: str, limit: int) -> str:
        return f"{cls.STATUS_KEY_PREFIX}:{cls.STATUS_VERSION}:{user_id}:limit:{limit}"

    @classmethod
    def _task_key(cls, *, user_id: str, limit: int) -> str:
        return f"{user_id}:limit:{limit}"

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _parse_datetime(value: object) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    @staticmethod
    def _parse_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        return str(value).strip().lower() in {"1", "true", "yes", "y"}
