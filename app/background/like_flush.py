"""
영화 좋아요 write-behind flush 스케줄러 작업
=================================================================

Redis `like:dirty` Hash 큐에 쌓인 좋아요 토글 이벤트를 주기적으로 드레인하여
MySQL `likes` 테이블에 배치 반영한다.

배경 (2026-04-07 이관):
  Backend(monglepick-backend)의 영화 좋아요 도메인이 recommend(FastAPI)로 이관되면서
  "하이브리드 write-behind" 정합성 패턴을 채택했다:
    1. 사용자 요청은 Redis에서 즉시 응답 (카운트 INCR/DECR, 사용자 셋 SADD/SREM)
    2. 실제 DB 반영은 이 백그라운드 작업이 주기적으로 처리
    3. Redis 장애 시 아직 flush되지 않은 토글 이력은 일부 손실될 수 있으나,
       카운트는 DB COUNT 쿼리로 lazy 복구 가능

Atomic drain 전략:
  1. `RENAME like:dirty → like:dirty:processing:{ts}` — Redis atomic 연산
     첫 번째 replica만 성공, 나머지는 source 없음 에러로 skip
  2. 스냅샷에서 HGETALL로 전체 dedup된 엔트리 조회
  3. 동일 (user_id, movie_id)에 대해 마지막 op만 채택 (Hash field 덮어쓰기 속성)
  4. LikeRepository.batch_apply_toggles로 MySQL 반영 (idempotent 설계)
  5. 성공 시 DEL processing key
  6. 실패 시 processing key를 보존 → 다음 주기에 재시도할 수 있도록 별도 복구 로직 고려

다중 replica 안전성:
  - RENAME은 Redis 레벨에서 원자적이므로 한 replica만 실질적으로 flush를 수행한다.
  - 동시에 실행되더라도 두 번째 RENAME은 "no such key" 예외로 즉시 no-op 종료.
  - processing key는 타임스탬프 suffix로 구분되므로 replica 간 충돌 없음.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.core.redis import get_redis
from app.core.scheduler import get_scheduler
from app.v2.core.database import get_pool
from app.v2.repository.like_repository import LikeRepository, ToggleOp
from app.v2.service.like_service import (
    DIRTY_PROCESSING_KEY_PREFIX,
    DIRTY_QUEUE_KEY,
    parse_dirty_field,
)

logger = logging.getLogger(__name__)


LIKE_FLUSH_JOB_ID = "like_write_behind_flush"


async def flush_like_dirty_queue() -> dict[str, Any]:
    """
    Redis `like:dirty` 큐를 드레인하여 MySQL `likes`에 반영한다.

    APScheduler에 등록되어 주기적으로 호출되며, 단일 실행 단위는 다음과 같다:
      1. processing key 생성 (timestamp suffix)
      2. RENAME `like:dirty` → processing key (atomic drain)
      3. HGETALL로 스냅샷 조회
      4. BATCH_MAX 초과 시 일부만 처리 (나머지는 다시 dirty로 되돌림)
      5. aiomysql 풀에서 커넥션 획득 → 트랜잭션 내 배치 반영
      6. 성공: DEL processing key / 실패: 원복

    Returns:
        dict: { "drained": int, "applied": int, "status": "ok|skip|error" }
    """
    settings = get_settings()
    processing_key = f"{DIRTY_PROCESSING_KEY_PREFIX}{int(time.time() * 1000)}"
    result: dict[str, Any] = {"drained": 0, "applied": 0, "status": "ok"}

    # ── Redis 클라이언트 확보 ──
    try:
        redis_client = await get_redis()
    except RuntimeError as exc:
        logger.warning("[like-flush] Redis 미초기화 — skip: %s", exc)
        result["status"] = "skip"
        return result

    # ── 1) atomic drain: RENAME ──
    try:
        await redis_client.rename(DIRTY_QUEUE_KEY, processing_key)
    except aioredis.ResponseError as exc:
        # "no such key" — flush할 엔트리가 없음. 정상 종료.
        msg = str(exc)
        if "no such key" in msg.lower():
            logger.debug("[like-flush] dirty 큐 비어 있음 — skip")
            result["status"] = "skip"
            return result
        # 그 외 Redis 에러
        logger.error("[like-flush] RENAME 실패: %s", exc)
        result["status"] = "error"
        return result
    except aioredis.RedisError as exc:
        logger.error("[like-flush] Redis RENAME 실패: %s", exc)
        result["status"] = "error"
        return result

    # ── 2) HGETALL 스냅샷 ──
    try:
        raw_entries = await redis_client.hgetall(processing_key)
    except aioredis.RedisError as exc:
        logger.error(
            "[like-flush] HGETALL 실패 (processing_key=%s): %s",
            processing_key, exc,
        )
        # 복구 시도: 원래 키로 되돌려 놓기
        await _restore_dirty_on_failure(redis_client, processing_key)
        result["status"] = "error"
        return result

    if not raw_entries:
        # 아주 드문 경로 — RENAME 직후 HGETALL 결과가 비어 있음 (empty hash였을 가능성)
        try:
            await redis_client.delete(processing_key)
        except aioredis.RedisError:
            pass
        result["status"] = "skip"
        return result

    # ── 3) dedup & BATCH_MAX 제한 ──
    # Redis Hash field 자체가 (user_id, movie_id) 단일 키이므로 이미 dedup 상태이지만,
    # 파싱과 함께 최종 op를 명확히 정한다.
    entries: list[tuple[str, str, ToggleOp]] = []
    for field, raw_value in raw_entries.items():
        try:
            payload = json.loads(raw_value)
            op_raw = payload.get("op", "").upper()
            if op_raw not in ("LIKE", "UNLIKE"):
                logger.warning(
                    "[like-flush] 잘못된 op 무시: field=%s value=%r",
                    field, raw_value,
                )
                continue
            user_id, movie_id = parse_dirty_field(field)
            if not user_id or not movie_id:
                logger.warning("[like-flush] 잘못된 field 형식 무시: %r", field)
                continue
            entries.append((user_id, movie_id, op_raw))  # type: ignore[arg-type]
        except (json.JSONDecodeError, TypeError, AttributeError) as exc:
            logger.warning(
                "[like-flush] 엔트리 파싱 실패 무시: field=%s value=%r err=%s",
                field, raw_value, exc,
            )
            continue

    result["drained"] = len(entries)

    # BATCH_MAX 초과 시 앞쪽만 처리하고 나머지는 원래 큐로 되돌림
    overflow: list[tuple[str, str, ToggleOp]] = []
    if len(entries) > settings.LIKE_FLUSH_BATCH_MAX:
        overflow = entries[settings.LIKE_FLUSH_BATCH_MAX:]
        entries = entries[: settings.LIKE_FLUSH_BATCH_MAX]
        logger.info(
            "[like-flush] 배치 최대치 초과 — 이번 사이클 %d건 처리, 다음 주기로 %d건 이월",
            len(entries), len(overflow),
        )

    # ── 4) MySQL 반영 ──
    try:
        applied = await _apply_entries_to_db(entries)
        result["applied"] = applied
    except Exception as exc:  # noqa: BLE001
        logger.exception("[like-flush] DB 반영 중 예외 — processing key 보존: %s", exc)
        # 실패 시 원래 큐로 되돌리기 시도 (overflow와 entries 모두)
        await _restore_dirty_on_failure(redis_client, processing_key)
        result["status"] = "error"
        return result

    # ── 5) processing key 삭제 + overflow 복구 ──
    try:
        await redis_client.delete(processing_key)
    except aioredis.RedisError as exc:
        logger.warning(
            "[like-flush] processing key DEL 실패 (무시): key=%s err=%s",
            processing_key, exc,
        )

    if overflow:
        # overflow 엔트리는 같은 필드명으로 다시 dirty 큐에 기록해 다음 주기에 처리
        try:
            mapping = {
                f"{user_id}|{movie_id}": json.dumps({"op": op, "ts": int(time.time())})
                for user_id, movie_id, op in overflow
            }
            if mapping:
                await redis_client.hset(DIRTY_QUEUE_KEY, mapping=mapping)
        except aioredis.RedisError as exc:
            logger.error("[like-flush] overflow 재큐잉 실패: %s", exc)

    logger.info(
        "[like-flush] 완료: drained=%d applied=%d overflow=%d",
        result["drained"], result["applied"], len(overflow),
    )
    return result


async def _apply_entries_to_db(
    entries: list[tuple[str, str, ToggleOp]],
) -> int:
    """
    aiomysql 풀에서 커넥션을 획득해 LikeRepository로 배치 반영.

    트랜잭션 정책: 전체 배치를 하나의 트랜잭션으로 묶어 all-or-nothing.
    개별 엔트리 실패는 `batch_apply_toggles` 내부에서 로그만 남기고 다음으로 진행한다.
    """
    if not entries:
        return 0

    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            repo = LikeRepository(conn)
            applied = await repo.batch_apply_toggles(entries)
            await conn.commit()
            return applied
        except Exception:
            await conn.rollback()
            raise


async def _restore_dirty_on_failure(
    redis_client: aioredis.Redis,
    processing_key: str,
) -> None:
    """
    DB 반영 실패 시 processing key의 엔트리를 원래 dirty 큐로 되돌린다.

    완벽한 정합성을 보장하지는 않지만 (중간에 다른 요청이 이미 같은 field를 갱신했을 수 있음)
    최선의 노력을 기울인다. Hash field는 키 충돌 시 기존 값이 덮어쓰이므로
    최종 상태는 "가장 최근 토글"로 수렴한다.
    """
    try:
        snapshot = await redis_client.hgetall(processing_key)
        if snapshot:
            await redis_client.hset(DIRTY_QUEUE_KEY, mapping=snapshot)
        await redis_client.delete(processing_key)
        logger.warning(
            "[like-flush] processing key 복구 완료: key=%s entries=%d",
            processing_key, len(snapshot) if snapshot else 0,
        )
    except aioredis.RedisError as exc:
        logger.error(
            "[like-flush] processing key 복구 실패 (수동 조치 필요): key=%s err=%s",
            processing_key, exc,
        )


def register_like_flush_job() -> None:
    """
    APScheduler에 like flush 잡을 등록한다.

    FastAPI lifespan 시작 시 호출한다.
    `LIKE_FLUSH_ENABLED=False` 이면 no-op.
    """
    settings = get_settings()
    if not settings.LIKE_FLUSH_ENABLED:
        logger.info("[like-flush] LIKE_FLUSH_ENABLED=False — 잡 등록 생략")
        return

    scheduler = get_scheduler()

    # 기존에 같은 ID로 등록돼 있으면 교체 (idempotent)
    if scheduler.get_job(LIKE_FLUSH_JOB_ID):
        scheduler.remove_job(LIKE_FLUSH_JOB_ID)

    scheduler.add_job(
        flush_like_dirty_queue,
        trigger="interval",
        seconds=settings.LIKE_FLUSH_INTERVAL_SECONDS,
        id=LIKE_FLUSH_JOB_ID,
        replace_existing=True,
        # 짧은 주기로 실행하는 잡이므로 이전 실행이 끝나지 않았으면 건너뛰기
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "[like-flush] APScheduler 잡 등록 완료: interval=%ds batch_max=%d",
        settings.LIKE_FLUSH_INTERVAL_SECONDS,
        settings.LIKE_FLUSH_BATCH_MAX,
    )
