"""
영화 좋아요 서비스 (v2 Raw SQL + Redis 하이브리드 캐시)
=================================================================

Backend(monglepick-backend)의 movie Like 도메인을 recommend(FastAPI)로 이관한 신규 구현.

아키텍처 (2026-04-07 확정):
  - 정합성 패턴: "하이브리드 write-behind"
      * 카운트 증감(INCR/DECR)과 사용자 좋아요 Set은 Redis에서 즉시 원자 갱신
      * 실제 DB 반영은 `app/background/like_flush.py` 스케줄러가 Redis dirty 큐를
        주기적으로 드레인하여 `LikeRepository.batch_apply_toggles`로 반영
      * Redis 장애 시 아직 flush되지 않은 토글 이력은 일부 손실될 수 있으나,
        카운트는 DB의 COUNT 쿼리로 lazy 리하이드레이션 가능

Redis 키 스키마:
  like:count:{movie_id}           String(int)  영화별 활성 좋아요 수 (캐시, 장기)
  like:user:{user_id}             Set<str>     사용자가 좋아요한 영화 ID 집합
  like:user:{user_id}:init        String("1")  사용자 셋 로드 완료 플래그 (TTL 1시간)
  like:dirty                      Hash         flush 대기 토글 큐
                                                field = f"{user_id}|{movie_id}"
                                                value = JSON {"op": "LIKE|UNLIKE", "ts": epoch}

핵심 규칙:
  - 'count' 키는 TTL 없음 — 장기 캐시. 단 EXISTS로 캐시 미스 감지 시 DB COUNT 재적재.
  - 'user' 셋과 init 플래그는 TTL 1시간. 만료 후 재조회 시 DB 재로드.
  - dirty 큐 키는 user와 movie가 '|'로 구분된다 (user_id/movie_id 모두 VARCHAR(50)이라
    파이프 문자가 들어갈 일이 없어 충돌 우려 없음).
  - 서비스 메서드는 DB 커넥션이 주입되지 않은 경우(None)에도 읽기 API가 동작하도록
    설계한다 — Redis 캐시가 모두 살아 있으면 DB 없이 응답 가능.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import aiomysql
import redis.asyncio as aioredis

from app.model.schema import LikeResponse
from app.v2.repository.like_repository import LikeRepository, ToggleOp

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# Redis 키 헬퍼 — 키 네이밍 일관성을 위해 한 곳에서 관리
# ─────────────────────────────────────────────────────────

# 모듈 레벨 상수 (flush 스케줄러에서도 동일 키를 사용해야 하므로 export)
DIRTY_QUEUE_KEY = "like:dirty"
DIRTY_PROCESSING_KEY_PREFIX = "like:dirty:processing:"
USER_SET_TTL_SECONDS = 3600  # 사용자 좋아요 셋 캐시 TTL (1시간)


def count_key(movie_id: str) -> str:
    """영화별 좋아요 수 캐시 키."""
    return f"like:count:{movie_id}"


def user_set_key(user_id: str) -> str:
    """사용자별 좋아요 영화 집합 키."""
    return f"like:user:{user_id}"


def user_init_key(user_id: str) -> str:
    """사용자 집합 초기화 완료 플래그 키."""
    return f"like:user:{user_id}:init"


def dirty_field(user_id: str, movie_id: str) -> str:
    """dirty 큐에 저장할 Hash field 이름."""
    return f"{user_id}|{movie_id}"


def parse_dirty_field(field: str) -> tuple[str, str]:
    """dirty 큐 field → (user_id, movie_id) 파싱."""
    user_id, _, movie_id = field.partition("|")
    return user_id, movie_id


# ─────────────────────────────────────────────────────────
# 서비스 본체
# ─────────────────────────────────────────────────────────

class LikeService:
    """
    영화 좋아요 하이브리드 캐시 서비스.

    Redis 캐시(count/user set/dirty 큐)를 기준으로 즉시 응답하고,
    실제 MySQL 반영은 write-behind 스케줄러가 처리한다.

    생성자의 `conn`은 Optional — Redis 캐시가 모두 살아 있을 때는 DB 없이도
    일부 읽기 API가 동작하도록 허용한다. 다만 캐시 미스 시 `conn is None`이면
    조회 실패를 의미하므로 0 또는 빈 응답으로 폴백한다.
    """

    def __init__(
        self,
        conn: Optional[aiomysql.Connection],
        redis_client: aioredis.Redis,
    ):
        self._conn = conn
        self._redis = redis_client
        self._repo: Optional[LikeRepository] = (
            LikeRepository(conn) if conn is not None else None
        )

    # ─────────────────────────────────────────────────────
    # Public API — 라우터에서 직접 호출하는 메서드
    # ─────────────────────────────────────────────────────

    async def toggle_like(self, user_id: str, movie_id: str) -> LikeResponse:
        """
        영화 좋아요를 토글한다 (등록/취소/복구).

        흐름:
          1. 사용자 좋아요 셋을 Redis에서 lazy 리하이드레이션
          2. 카운트 캐시도 lazy 리하이드레이션 (응답용)
          3. SISMEMBER로 현재 상태 판정
             - 있음(활성) → SREM + DECR + dirty 큐에 UNLIKE 기록
             - 없음(미좋아요) → SADD + INCR + dirty 큐에 LIKE 기록
          4. 즉시 응답 (DB 반영은 스케줄러가 나중에)

        주의:
          - Redis 장애 시 예외가 발생하면 DB 동기 fallback으로 응답한다.
            이 경우 write-behind 큐에도 아무것도 기록되지 않는다 (DB가 진실 원본이 됨).
        """
        try:
            return await self._toggle_via_cache(user_id, movie_id)
        except aioredis.RedisError as exc:
            logger.warning(
                "toggle_like Redis 실패 → DB 동기 폴백: user=%s movie=%s err=%s",
                user_id, movie_id, exc,
            )
            return await self._toggle_via_db_only(user_id, movie_id)

    async def is_liked(self, user_id: str, movie_id: str) -> LikeResponse:
        """
        (GET) 사용자의 해당 영화 활성 좋아요 여부 + 전체 카운트.

        Backend의 `GET /api/v1/movies/{movieId}/like` 응답과 동일한 구조로 반환.
        """
        try:
            await self._rehydrate_user_set(user_id)
            liked_raw = await self._redis.sismember(user_set_key(user_id), movie_id)
            liked = bool(liked_raw)
            count = await self._get_count(movie_id)
            return LikeResponse(liked=liked, like_count=count)
        except aioredis.RedisError as exc:
            logger.warning(
                "is_liked Redis 실패 → DB 폴백: user=%s movie=%s err=%s",
                user_id, movie_id, exc,
            )
            if self._repo is None:
                return LikeResponse(liked=False, like_count=0)
            existing = await self._repo.find_by_user_movie(user_id, movie_id)
            liked = bool(existing and existing.deleted_at is None)
            count = await self._repo.count_active_by_movie(movie_id)
            return LikeResponse(liked=liked, like_count=count)

    async def get_count(self, movie_id: str) -> LikeResponse:
        """
        (GET, 공개) 영화의 활성 좋아요 수만 반환.

        Backend처럼 `liked` 필드는 항상 false 고정.
        """
        try:
            count = await self._get_count(movie_id)
            return LikeResponse(liked=False, like_count=count)
        except aioredis.RedisError as exc:
            logger.warning("get_count Redis 실패 → DB 폴백: movie=%s err=%s", movie_id, exc)
            if self._repo is None:
                return LikeResponse(liked=False, like_count=0)
            count = await self._repo.count_active_by_movie(movie_id)
            return LikeResponse(liked=False, like_count=count)

    # ─────────────────────────────────────────────────────
    # 내부 — 토글 로직
    # ─────────────────────────────────────────────────────

    async def _toggle_via_cache(self, user_id: str, movie_id: str) -> LikeResponse:
        """Redis 캐시 기반 토글 (정상 경로)."""
        await self._rehydrate_user_set(user_id)
        # 응답용 현재 카운트 보장
        await self._rehydrate_count_if_missing(movie_id)

        uset_key = user_set_key(user_id)
        c_key = count_key(movie_id)
        was_liked_raw = await self._redis.sismember(uset_key, movie_id)
        was_liked = bool(was_liked_raw)

        if was_liked:
            # 활성 → 취소 (UNLIKE)
            await self._redis.srem(uset_key, movie_id)
            # DECR은 음수로 가지 않도록 사후 방어
            new_count_raw = await self._redis.decr(c_key)
            new_count = int(new_count_raw or 0)
            if new_count < 0:
                # 극단 케이스: 카운트 리하이드레이션이 부정확했을 때 0으로 보정
                logger.warning(
                    "like count 음수 감지 → 0 보정: movie=%s new_count=%d",
                    movie_id, new_count,
                )
                await self._redis.set(c_key, 0)
                new_count = 0
            await self._enqueue_dirty(user_id, movie_id, "UNLIKE")
            return LikeResponse(liked=False, like_count=new_count)

        # 미좋아요 → 등록 (LIKE)
        await self._redis.sadd(uset_key, movie_id)
        new_count_raw = await self._redis.incr(c_key)
        new_count = int(new_count_raw or 1)
        await self._enqueue_dirty(user_id, movie_id, "LIKE")
        return LikeResponse(liked=True, like_count=new_count)

    async def _toggle_via_db_only(
        self, user_id: str, movie_id: str,
    ) -> LikeResponse:
        """
        Redis 장애 시 동기 폴백 — DB에 직접 INSERT/UPDATE.

        이 경로는 dirty 큐를 거치지 않으므로 DB가 곧바로 진실 원본이 된다.
        따라서 다음 요청에서 Redis가 복구되면 카운트/셋이 lazy 재로드된다.
        """
        if self._repo is None:
            raise RuntimeError("Redis와 DB 모두 사용 불가 — toggle_like 불가")

        existing = await self._repo.find_by_user_movie(user_id, movie_id)
        if existing is None:
            await self._repo.apply_toggle(user_id, movie_id, "LIKE")
            liked = True
        elif existing.deleted_at is None:
            await self._repo.apply_toggle(user_id, movie_id, "UNLIKE")
            liked = False
        else:
            await self._repo.apply_toggle(user_id, movie_id, "LIKE")
            liked = True
        count = await self._repo.count_active_by_movie(movie_id)
        # DB 폴백 응답 — 트랜잭션 커밋은 FastAPI `get_conn` 의존성이 처리
        return LikeResponse(liked=liked, like_count=count)

    # ─────────────────────────────────────────────────────
    # 내부 — 리하이드레이션 / 캐시 초기화
    # ─────────────────────────────────────────────────────

    async def _rehydrate_user_set(self, user_id: str) -> None:
        """
        사용자 좋아요 셋 캐시가 비어 있으면(초기화 안됐으면) DB에서 로드한다.

        Redis SET은 "존재하지 않음"과 "빈 셋"을 구분하기 어려우므로 별도의
        init 플래그 키(`like:user:{user_id}:init`)로 "로드 완료" 상태를 관리한다.

        - init 플래그 키 존재 → 캐시 신뢰 (트러스트)
        - init 플래그 키 없음 → DB에서 로드 + SADD + init 플래그 세팅 (TTL 1시간)
        """
        ik = user_init_key(user_id)
        try:
            init_exists = await self._redis.exists(ik)
        except aioredis.RedisError:
            raise

        if init_exists:
            return  # 이미 로드됨

        if self._repo is None:
            # DB 없이 초기화 — 빈 셋으로 간주 (쓰기에 의존)
            await self._redis.set(ik, "1", ex=USER_SET_TTL_SECONDS)
            await self._redis.expire(user_set_key(user_id), USER_SET_TTL_SECONDS)
            return

        liked_ids = await self._repo.list_active_movie_ids_by_user(user_id)
        uset_key = user_set_key(user_id)

        # 기존 스테일 셋 제거 (TTL 만료 직전 일부 잔여가 있을 수 있음)
        await self._redis.delete(uset_key)
        if liked_ids:
            await self._redis.sadd(uset_key, *liked_ids)
        # 셋과 init 플래그 둘 다 TTL 설정
        await self._redis.expire(uset_key, USER_SET_TTL_SECONDS)
        await self._redis.set(ik, "1", ex=USER_SET_TTL_SECONDS)
        logger.debug(
            "rehydrate user set from DB: user=%s loaded=%d",
            user_id, len(liked_ids),
        )

    async def _rehydrate_count_if_missing(self, movie_id: str) -> int:
        """
        카운트 캐시가 없으면 DB COUNT로 초기화. 현재 카운트를 반환.
        """
        c_key = count_key(movie_id)
        try:
            cur = await self._redis.get(c_key)
        except aioredis.RedisError:
            raise

        if cur is not None:
            try:
                return int(cur)
            except (TypeError, ValueError):
                # 비정상 값 감지 — 초기화로 진행
                logger.warning("like count 비정상 값 감지 → 재초기화: movie=%s raw=%r", movie_id, cur)

        if self._repo is None:
            # DB 없음 — 0으로 초기화 (쓰기 트래픽에 의존)
            await self._redis.set(c_key, 0)
            return 0

        count = await self._repo.count_active_by_movie(movie_id)
        # SET NX를 쓰지 않는 이유: 같은 키에 여러 요청이 동시에 들어와도
        # DB 값이 정답이므로 덮어써도 무방하다. 다만 카운터 원자 갱신 중에는
        # 드물게 "SET 0 → 동시 INCR → SET 정답"으로 인한 카운트 드리프트가
        # 발생할 수 있어, 이미 키가 있으면 건드리지 않는다.
        was_set = await self._redis.set(c_key, count, nx=True)
        if not was_set:
            # 다른 요청이 먼저 초기화함 → 그 값을 사용
            cur2 = await self._redis.get(c_key)
            try:
                return int(cur2) if cur2 is not None else count
            except (TypeError, ValueError):
                return count
        logger.debug("rehydrate count from DB: movie=%s count=%d", movie_id, count)
        return count

    async def _get_count(self, movie_id: str) -> int:
        """카운트 조회 + 캐시 미스 시 리하이드레이션."""
        return await self._rehydrate_count_if_missing(movie_id)

    # ─────────────────────────────────────────────────────
    # 내부 — dirty 큐 기록
    # ─────────────────────────────────────────────────────

    async def _enqueue_dirty(
        self, user_id: str, movie_id: str, op: ToggleOp,
    ) -> None:
        """
        write-behind 큐에 토글 이벤트를 기록한다.

        동일 (user_id, movie_id)에 대해 빠르게 다시 토글되면 기존 field를
        최신 op로 덮어쓴다 → flush 시 최종 상태만 반영된다.
        """
        payload = json.dumps({"op": op, "ts": int(time.time())})
        field = dirty_field(user_id, movie_id)
        await self._redis.hset(DIRTY_QUEUE_KEY, field, payload)
