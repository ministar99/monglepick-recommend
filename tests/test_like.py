"""
영화 좋아요 서비스 단위 테스트 (v2 하이브리드 캐시)
=================================================================

목적:
  Backend → recommend 이관된 영화 좋아요의 "Redis 즉시 반영 + write-behind flush"
  패턴이 설계대로 동작하는지 검증한다.

테스트 전략:
  - LikeRepository는 AsyncMock으로 대체 (실제 MySQL 없이 흐름만 검증)
  - Redis는 tests/conftest.py의 FakeRedis 사용 (Set/Hash/INCR/DECR 지원)
  - dirty 큐에 정확한 형식의 엔트리가 쌓이는지, count/user set이 원자 갱신되는지 확인
  - flush 배치 로직은 별도 테스트로 Mock repo를 감시

비검증 범위:
  - 실제 MySQL 쿼리 실행은 v2 test 인프라 미비로 검증하지 않음
    (SearchHistory v2 repository도 MySQL 기반이라 현재 저장소에 v2 통합 테스트 없음)
  - Nginx 프록시 동작은 운영 환경 검증
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import redis.asyncio as aioredis

from app.v2.model.dto import LikeDTO
from app.v2.service.like_service import (
    DIRTY_PROCESSING_KEY_PREFIX,
    DIRTY_QUEUE_KEY,
    LikeService,
    count_key,
    dirty_field,
    parse_dirty_field,
    user_init_key,
    user_set_key,
)


# ─────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────

def _make_mock_repo(
    *,
    find_by_user_movie=None,
    count_active_by_movie=None,
    list_active_movie_ids_by_user=None,
    apply_toggle=None,
) -> AsyncMock:
    """LikeRepository 모킹 헬퍼. 필요한 메서드만 재정의."""
    repo = AsyncMock()
    repo.find_by_user_movie = AsyncMock(
        return_value=find_by_user_movie if find_by_user_movie is not None else None
    )
    repo.count_active_by_movie = AsyncMock(
        return_value=count_active_by_movie if count_active_by_movie is not None else 0
    )
    repo.list_active_movie_ids_by_user = AsyncMock(
        return_value=list_active_movie_ids_by_user if list_active_movie_ids_by_user is not None else [],
    )
    repo.apply_toggle = AsyncMock(
        return_value=apply_toggle if apply_toggle is not None else None
    )
    return repo


def _build_service(fake_redis, repo_mock) -> LikeService:
    """
    LikeService 생성 + 내부 _repo를 mock으로 강제 주입.

    conn 파라미터는 None으로 넘겨 기본 repo 생성을 피하고,
    테스트가 직접 모킹한 repo를 붙인다.
    """
    service = LikeService(conn=None, redis_client=fake_redis)
    service._repo = repo_mock  # type: ignore[attr-defined]
    return service


# ─────────────────────────────────────────
# LikeService.toggle_like — 신규/취소/복구 시나리오
# ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_toggle_like_신규등록_리하이드레이션_없는_상태(fake_redis):
    """
    캐시가 비어 있는 상태에서 신규 좋아요를 누르면:
      - 사용자 셋 리하이드레이션(DB 조회) 발생
      - 카운트 리하이드레이션(DB COUNT) 발생
      - Redis에 SADD + INCR 원자 반영
      - dirty 큐에 op=LIKE 기록
      - 응답: liked=True, likeCount=DB초기값+1
    """
    repo = _make_mock_repo(
        list_active_movie_ids_by_user=[],   # 해당 사용자는 아직 좋아요한 영화 없음
        count_active_by_movie=3,            # 이 영화는 기존에 3명이 좋아요
    )
    service = _build_service(fake_redis, repo)

    result = await service.toggle_like("user_A", "movie_1")

    # 응답 검증
    assert result.liked is True
    assert result.like_count == 4  # 3 → INCR → 4

    # Redis 셋 검증 — movie_1이 user_A 좋아요 셋에 들어갔는지
    assert await fake_redis.sismember(user_set_key("user_A"), "movie_1") == 1

    # init 플래그가 세팅되었는지
    assert await fake_redis.exists(user_init_key("user_A")) == 1

    # 카운트 캐시 검증
    assert await fake_redis.get(count_key("movie_1")) == "4"

    # dirty 큐에 LIKE 기록 검증
    raw = await fake_redis.hget(DIRTY_QUEUE_KEY, dirty_field("user_A", "movie_1"))
    assert raw is not None
    payload = json.loads(raw)
    assert payload["op"] == "LIKE"

    # DB 호출 검증 — 리하이드레이션 시 repo 호출은 있었지만 apply_toggle은 호출 안 됨
    repo.list_active_movie_ids_by_user.assert_awaited_once_with("user_A")
    repo.count_active_by_movie.assert_awaited_once_with("movie_1")
    repo.apply_toggle.assert_not_called()


@pytest.mark.asyncio
async def test_toggle_like_취소_기존_활성_상태(fake_redis):
    """
    이미 활성 좋아요 상태에서 토글하면:
      - 리하이드레이션에서 셋에 movie_1이 포함되어 로드됨
      - SREM + DECR 반영
      - dirty 큐에 op=UNLIKE 기록
      - 응답: liked=False, likeCount=초기값-1
    """
    repo = _make_mock_repo(
        list_active_movie_ids_by_user=["movie_1"],  # user_A는 이미 movie_1 좋아요
        count_active_by_movie=10,
    )
    service = _build_service(fake_redis, repo)

    result = await service.toggle_like("user_A", "movie_1")

    assert result.liked is False
    assert result.like_count == 9  # 10 → DECR → 9
    assert await fake_redis.sismember(user_set_key("user_A"), "movie_1") == 0
    assert await fake_redis.get(count_key("movie_1")) == "9"

    raw = await fake_redis.hget(DIRTY_QUEUE_KEY, dirty_field("user_A", "movie_1"))
    payload = json.loads(raw)
    assert payload["op"] == "UNLIKE"


@pytest.mark.asyncio
async def test_toggle_like_복구_LIKE_UNLIKE_LIKE_연속(fake_redis):
    """
    LIKE → UNLIKE → LIKE 연속 토글 시:
      - 각 요청마다 응답이 정확히 번갈아가며 반환되고
      - dirty 큐의 최종 field value는 마지막 op(LIKE)만 남아야 한다
        (같은 field에 HSET하면 이전 값이 덮어써지므로 Hash 속성상 자연스러운 dedup)
      - 카운트는 DB 초기값 대비 +1 (최종적으로 liked 상태이므로)
    """
    repo = _make_mock_repo(
        list_active_movie_ids_by_user=[],
        count_active_by_movie=5,
    )
    service = _build_service(fake_redis, repo)

    r1 = await service.toggle_like("user_A", "movie_X")  # +1 → 6
    assert r1.liked is True and r1.like_count == 6
    r2 = await service.toggle_like("user_A", "movie_X")  # -1 → 5
    assert r2.liked is False and r2.like_count == 5
    r3 = await service.toggle_like("user_A", "movie_X")  # +1 → 6
    assert r3.liked is True and r3.like_count == 6

    # dirty 큐 field는 하나만 있어야 하고, op=LIKE여야 함
    dirty = await fake_redis.hgetall(DIRTY_QUEUE_KEY)
    expected_field = dirty_field("user_A", "movie_X")
    assert list(dirty.keys()) == [expected_field]
    assert json.loads(dirty[expected_field])["op"] == "LIKE"

    # 리하이드레이션은 처음 한 번만 수행되어야 (init 플래그 덕분에)
    assert repo.list_active_movie_ids_by_user.await_count == 1
    # 카운트 리하이드레이션도 처음 한 번만 (NX로 보호됨)
    assert repo.count_active_by_movie.await_count == 1


@pytest.mark.asyncio
async def test_toggle_like_카운트_음수_방어(fake_redis):
    """
    Redis 카운트가 잘못된 초기값(예: DB 복구 오류로 0)에서 UNLIKE가 오면,
    DECR 결과가 음수가 되더라도 서비스는 이를 0으로 보정해야 한다.
    """
    # 사용자 셋에는 movie_1이 있다고 가정(이미 좋아요 상태)
    repo = _make_mock_repo(
        list_active_movie_ids_by_user=["movie_1"],
        count_active_by_movie=0,  # DB가 0이라고 답한 경우
    )
    service = _build_service(fake_redis, repo)

    result = await service.toggle_like("user_A", "movie_1")

    assert result.liked is False
    assert result.like_count == 0  # 음수 방어로 0 보정
    assert await fake_redis.get(count_key("movie_1")) == "0"


# ─────────────────────────────────────────
# LikeService.is_liked / get_count
# ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_is_liked_캐시_리하이드레이션(fake_redis):
    """is_liked 호출 시 리하이드레이션으로 DB에서 사용자 셋을 로드해 판정."""
    repo = _make_mock_repo(
        list_active_movie_ids_by_user=["movie_1", "movie_2"],
        count_active_by_movie=7,
    )
    service = _build_service(fake_redis, repo)

    r1 = await service.is_liked("user_A", "movie_1")
    assert r1.liked is True
    assert r1.like_count == 7

    r2 = await service.is_liked("user_A", "movie_3")  # 좋아요한 적 없음
    assert r2.liked is False


@pytest.mark.asyncio
async def test_get_count_공개_API(fake_redis):
    """get_count는 항상 liked=False, likeCount만 채워 반환."""
    repo = _make_mock_repo(count_active_by_movie=42)
    service = _build_service(fake_redis, repo)

    result = await service.get_count("movie_1")

    assert result.liked is False
    assert result.like_count == 42
    # 사용자 셋은 건드리지 않음
    repo.list_active_movie_ids_by_user.assert_not_called()


@pytest.mark.asyncio
async def test_get_count_캐시_히트_DB_호출_없음(fake_redis):
    """카운트 캐시가 이미 있으면 DB COUNT를 호출하지 않음."""
    repo = _make_mock_repo(count_active_by_movie=999)  # 이 값이 사용되면 테스트 실패
    service = _build_service(fake_redis, repo)

    # 캐시 선세팅
    await fake_redis.set(count_key("movie_1"), 15)

    result = await service.get_count("movie_1")

    assert result.like_count == 15
    repo.count_active_by_movie.assert_not_called()


# ─────────────────────────────────────────
# LikeResponse JSON 직렬화 (Backend 호환성)
# ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_like_response_camelcase_직렬화():
    """
    LikeResponse는 Backend(Spring Boot) 응답과 동일하게 camelCase(`likeCount`)로
    직렬화되어야 한다. 이것이 깨지면 Nginx 라우팅 후 Frontend 호환성이 망가진다.
    """
    from app.model.schema import LikeResponse

    resp = LikeResponse(liked=True, like_count=42)
    dumped = resp.model_dump(by_alias=True)
    assert dumped == {"liked": True, "likeCount": 42}


# ─────────────────────────────────────────
# Redis 장애 시 DB 동기 폴백
# ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_toggle_like_Redis_장애_시_DB_폴백(fake_redis):
    """
    Redis 호출이 RedisError를 던지면 LikeService는 DB 동기 경로로 폴백해야 한다.
    폴백 시에는 dirty 큐를 거치지 않고 repo.apply_toggle을 직접 호출한다.
    """
    # Redis exists를 예외로 대체
    async def raise_redis_error(*args, **kwargs):
        raise aioredis.RedisError("simulated failure")

    fake_redis.exists = raise_redis_error  # type: ignore[assignment]

    # 기존 상태: 레코드 없음 → 신규 LIKE
    repo = _make_mock_repo(
        find_by_user_movie=None,
        count_active_by_movie=1,
    )
    service = _build_service(fake_redis, repo)

    result = await service.toggle_like("user_A", "movie_1")

    assert result.liked is True
    assert result.like_count == 1
    repo.apply_toggle.assert_awaited_once_with("user_A", "movie_1", "LIKE")


@pytest.mark.asyncio
async def test_toggle_like_DB_폴백_기존활성은_UNLIKE(fake_redis):
    """
    Redis 장애 폴백 시, DB에 활성 레코드가 있으면 UNLIKE로 수렴.
    """
    async def raise_redis_error(*args, **kwargs):
        raise aioredis.RedisError("simulated failure")

    fake_redis.exists = raise_redis_error  # type: ignore[assignment]

    existing = LikeDTO(
        like_id=1,
        user_id="user_A",
        movie_id="movie_1",
        deleted_at=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    repo = _make_mock_repo(
        find_by_user_movie=existing,
        count_active_by_movie=0,  # 취소 후 0명
    )
    service = _build_service(fake_redis, repo)

    result = await service.toggle_like("user_A", "movie_1")

    assert result.liked is False
    assert result.like_count == 0
    repo.apply_toggle.assert_awaited_once_with("user_A", "movie_1", "UNLIKE")


# ─────────────────────────────────────────
# dirty 큐 필드 파싱 유틸
# ─────────────────────────────────────────

def test_parse_dirty_field_정상():
    assert parse_dirty_field("user_123|movie_456") == ("user_123", "movie_456")


def test_parse_dirty_field_빈값():
    assert parse_dirty_field("|") == ("", "")
    # movie_id에 파이프가 없으면 잘못된 입력이지만 방어적 파싱
    assert parse_dirty_field("only_user") == ("only_user", "")


# ─────────────────────────────────────────
# like_flush (write-behind) 스케줄러 잡
# ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_like_flush_큐_비어있을_때_skip(fake_redis, monkeypatch):
    """dirty 큐가 비어 있으면 RENAME이 실패하고 status=skip 을 반환한다."""
    from app.background import like_flush

    # get_redis가 우리 fake를 반환하도록 monkeypatch
    async def _get_redis():
        return fake_redis
    monkeypatch.setattr(like_flush, "get_redis", _get_redis)

    result = await like_flush.flush_like_dirty_queue()

    assert result["status"] == "skip"
    assert result["drained"] == 0


@pytest.mark.asyncio
async def test_like_flush_엔트리_정상_반영(fake_redis, monkeypatch):
    """
    dirty 큐에 엔트리가 있으면 RENAME → HGETALL → repo.batch_apply_toggles 호출이
    이루어지고 processing key가 삭제되어야 한다.
    """
    from app.background import like_flush

    # dirty 큐에 두 건 선세팅
    await fake_redis.hset(
        DIRTY_QUEUE_KEY,
        mapping={
            dirty_field("user_A", "movie_1"): json.dumps({"op": "LIKE", "ts": 1}),
            dirty_field("user_B", "movie_2"): json.dumps({"op": "UNLIKE", "ts": 2}),
        },
    )

    # Redis getter monkeypatch
    async def _get_redis():
        return fake_redis
    monkeypatch.setattr(like_flush, "get_redis", _get_redis)

    # aiomysql pool 우회 — _apply_entries_to_db 전체를 대체
    applied_entries: list = []

    async def _fake_apply(entries):
        applied_entries.extend(entries)
        return len(entries)

    monkeypatch.setattr(like_flush, "_apply_entries_to_db", _fake_apply)

    result = await like_flush.flush_like_dirty_queue()

    assert result["status"] == "ok"
    assert result["drained"] == 2
    assert result["applied"] == 2

    # 순서 무관하게 포함되어야
    assert ("user_A", "movie_1", "LIKE") in applied_entries
    assert ("user_B", "movie_2", "UNLIKE") in applied_entries

    # processing key는 삭제되었어야 하고 dirty 큐도 비어 있어야 함
    assert await fake_redis.hlen(DIRTY_QUEUE_KEY) == 0
    # 어떤 processing key도 남아 있으면 안 됨
    any_processing = any(
        k.startswith(DIRTY_PROCESSING_KEY_PREFIX)
        for k in list(fake_redis._hashes.keys()) + list(fake_redis._store.keys())
    )
    assert not any_processing


@pytest.mark.asyncio
async def test_like_flush_DB_실패_시_processing_key_복구(fake_redis, monkeypatch):
    """
    DB 반영 중 예외 발생 시 processing key의 엔트리가 dirty 큐로 복구되어야 한다.
    다음 주기에 재시도할 수 있도록.
    """
    from app.background import like_flush

    await fake_redis.hset(
        DIRTY_QUEUE_KEY,
        mapping={
            dirty_field("user_A", "movie_1"): json.dumps({"op": "LIKE", "ts": 1}),
        },
    )

    async def _get_redis():
        return fake_redis
    monkeypatch.setattr(like_flush, "get_redis", _get_redis)

    async def _raise_db_error(entries):
        raise RuntimeError("simulated DB failure")

    monkeypatch.setattr(like_flush, "_apply_entries_to_db", _raise_db_error)

    result = await like_flush.flush_like_dirty_queue()

    assert result["status"] == "error"
    # 엔트리는 원래 dirty 큐로 복구되어야 함
    restored = await fake_redis.hgetall(DIRTY_QUEUE_KEY)
    assert dirty_field("user_A", "movie_1") in restored
