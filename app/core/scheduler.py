"""
백그라운드 스케줄러 싱글턴 (AsyncIOScheduler)
=================================================================

FastAPI 애플리케이션에 주기적 백그라운드 작업을 등록하는 단일 진입점.

최초 도입: 2026-04-07
도입 배경:
  Backend(monglepick-backend)에서 recommend(FastAPI)로 이관된 영화 좋아요 기능이
  "Redis 즉시 반영 + DB 주기 flush (write-behind)" 패턴을 채택함에 따라,
  주기적 Redis 드레인 → MySQL 배치 반영 작업을 실행할 스케줄러가 필요해졌다.

설계 결정:
  1. APScheduler의 AsyncIOScheduler를 사용 (FastAPI 기본 event loop 공유)
  2. 모듈 레벨 싱글턴으로 관리 (FastAPI lifespan에서 start/shutdown 제어)
  3. 작업 등록은 `init_scheduler()`에서 일괄 처리
  4. 다중 replica 환경에서는 각 인스턴스가 자체 스케줄러를 실행하지만,
     flush 로직 자체가 Redis `RENAME`으로 atomic drain을 하므로 중복 처리 없음
     (첫 번째 replica가 RENAME 성공하면 나머지는 source 없음 에러로 skip)
"""

from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 모듈 레벨 싱글턴
# ─────────────────────────────────────────
_scheduler: Optional[AsyncIOScheduler] = None


def get_scheduler() -> AsyncIOScheduler:
    """
    스케줄러 인스턴스를 반환한다 (지연 초기화).

    최초 호출 시 AsyncIOScheduler를 생성하여 모듈 변수에 저장한다.
    두 번째 호출부터는 같은 인스턴스를 반환한다.
    """
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(
            # timezone은 시스템 기본을 그대로 사용 (좋아요 flush는 주기만 중요)
            timezone="UTC",
        )
    return _scheduler


def start_scheduler() -> None:
    """
    스케줄러를 시작한다.

    FastAPI lifespan 시작 시 호출한다. 이미 실행 중이면 no-op.
    잡 등록은 별도 함수(`register_like_flush_job` 등)에서 수행한다.
    """
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("[scheduler] AsyncIOScheduler 시작됨")
    else:
        logger.debug("[scheduler] 이미 실행 중 — start() 생략")


async def shutdown_scheduler() -> None:
    """
    스케줄러를 종료한다 (진행 중 작업 완료까지 대기).

    FastAPI lifespan 종료 시 호출한다. 실행 중이 아니면 no-op.
    """
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=True)
        logger.info("[scheduler] AsyncIOScheduler 종료됨")
    _scheduler = None
