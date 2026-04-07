"""
테스트 공통 픽스처

SQLite 인메모리 DB와 FakeRedis를 사용하여
외부 의존성 없이 테스트를 실행할 수 있도록 합니다.

주요 픽스처:
- async_session: SQLAlchemy 비동기 세션 (SQLite 인메모리)
- fake_redis: FakeRedis 대용 (실제 Redis 불필요)
- client: FastAPI TestClient (httpx AsyncClient)
- auth_headers: JWT 인증 헤더 (테스트용 토큰)

DDL 기준: user_id VARCHAR(50) PK, movie_id VARCHAR(50) PK
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import StaticPool
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.core.database import Base
from app.main import app
from app.api.deps import get_db, get_redis_client, get_current_user, get_current_user_optional


# ─────────────────────────────────────────
# 테스트용 SQLite 비동기 엔진 (인메모리)
# ─────────────────────────────────────────
# aiosqlite를 사용하여 SQLite 비동기 엔진 생성
# StaticPool: 모든 커넥션이 동일한 인메모리 DB를 공유
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestSessionFactory = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ─────────────────────────────────────────
# 테스트용 사용자 ID (DDL 기준: VARCHAR(50))
# ─────────────────────────────────────────
TEST_USER_ID = "test_user_1"
TEST_USER_EMAIL = "test@monglepick.com"


def create_test_token(user_id: str = TEST_USER_ID) -> str:
    """
    테스트용 JWT 토큰을 생성합니다.

    Spring Boot 백엔드가 발급하는 것과 동일한 구조입니다.
    sub 클레임에 user_id 문자열을 저장합니다.

    Args:
        user_id: 테스트 사용자 ID (VARCHAR(50))

    Returns:
        JWT 토큰 문자열
    """
    settings = get_settings()
    payload = {
        "sub": user_id,
        "email": TEST_USER_EMAIL,
        "role": "USER",
        "iat": datetime.now(tz=timezone.utc),
        "exp": datetime.now(tz=timezone.utc) + timedelta(hours=1),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


# ─────────────────────────────────────────
# FakeRedis: Redis 명령을 인메모리로 시뮬레이션
# ─────────────────────────────────────────
class FakeRedis:
    """
    테스트용 Redis 모의 객체

    실제 Redis 없이 기본적인 명령(GET, SET, ZADD, ZREVRANGE 등)을
    인메모리 딕셔너리로 시뮬레이션합니다.

    2026-04-07 확장: 좋아요 write-behind 테스트를 위해 Set/Hash/Counter/Rename 지원 강화
      - String: get, set(NX), setex, incr, decr, delete, exists
      - Set: sadd, srem, sismember, smembers, scard
      - Hash: hset, hgetall, hdel, hlen, hget
      - 기타: rename(atomic), ping, expire, zincrby, zrevrange
    """

    def __init__(self):
        self._store: dict[str, str] = {}  # String 저장소
        self._sorted_sets: dict[str, dict[str, float]] = {}  # Sorted Set 저장소
        self._hashes: dict[str, dict[str, str]] = {}  # Hash 저장소
        self._sets: dict[str, set[str]] = {}  # Set 저장소 (2026-04-07 추가)
        self._ttls: dict[str, int] = {}  # TTL 저장소

    # ─────────────────────────────────
    # String commands
    # ─────────────────────────────────
    async def get(self, key: str) -> str | None:
        """String GET"""
        return self._store.get(key)

    async def set(self, key: str, value, ex: int | None = None, nx: bool = False) -> bool | None:
        """
        String SET

        - nx=True: 키가 이미 존재하면 실패 (None 반환)
        - ex: TTL (초)
        """
        if nx and key in self._store:
            return None
        self._store[key] = str(value)
        if ex is not None:
            self._ttls[key] = ex
        return True

    async def setex(self, key: str, ttl: int, value) -> None:
        """String SET with TTL"""
        self._store[key] = str(value)
        self._ttls[key] = ttl

    async def incr(self, key: str) -> int:
        """String INCR — 없으면 0에서 시작해 +1"""
        current = int(self._store.get(key, "0"))
        current += 1
        self._store[key] = str(current)
        return current

    async def decr(self, key: str) -> int:
        """String DECR — 없으면 0에서 시작해 -1"""
        current = int(self._store.get(key, "0"))
        current -= 1
        self._store[key] = str(current)
        return current

    async def exists(self, key: str) -> int:
        """키 존재 여부 (0 또는 1). 실제 Redis는 멀티 키 지원하지만 단일 키만 흉내."""
        if key in self._store or key in self._sets or key in self._hashes or key in self._sorted_sets:
            return 1
        return 0

    async def delete(self, key: str) -> int:
        """키 삭제 (모든 타입 스토어에서 제거)"""
        deleted = 0
        if key in self._store:
            del self._store[key]
            deleted += 1
        if key in self._sorted_sets:
            del self._sorted_sets[key]
            deleted += 1
        if key in self._hashes:
            del self._hashes[key]
            deleted += 1
        if key in self._sets:
            del self._sets[key]
            deleted += 1
        self._ttls.pop(key, None)
        return deleted

    async def rename(self, src: str, dst: str) -> bool:
        """
        RENAME src → dst (atomic).

        실제 Redis는 src가 없으면 `ResponseError("no such key")`를 raise 한다.
        테스트에서도 동일한 예외를 발생시켜 flush의 skip 로직을 검증할 수 있게 한다.
        """
        src_type = None
        if src in self._hashes:
            src_type = "hash"
        elif src in self._store:
            src_type = "string"
        elif src in self._sets:
            src_type = "set"
        elif src in self._sorted_sets:
            src_type = "zset"

        if src_type is None:
            import redis.asyncio as aioredis
            raise aioredis.ResponseError("no such key")

        # dst 기존 데이터 제거
        await self.delete(dst)

        if src_type == "hash":
            self._hashes[dst] = self._hashes.pop(src)
        elif src_type == "string":
            self._store[dst] = self._store.pop(src)
        elif src_type == "set":
            self._sets[dst] = self._sets.pop(src)
        elif src_type == "zset":
            self._sorted_sets[dst] = self._sorted_sets.pop(src)
        # TTL도 이전 (간단화)
        if src in self._ttls:
            self._ttls[dst] = self._ttls.pop(src)
        return True

    # ─────────────────────────────────
    # Sorted Set commands
    # ─────────────────────────────────
    async def zincrby(self, key: str, amount: float, member: str) -> float:
        """Sorted Set ZINCRBY"""
        if key not in self._sorted_sets:
            self._sorted_sets[key] = {}
        current = self._sorted_sets[key].get(member, 0.0)
        self._sorted_sets[key][member] = current + amount
        return self._sorted_sets[key][member]

    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> list:
        """Sorted Set ZREVRANGE (score 내림차순)"""
        if key not in self._sorted_sets:
            return []
        sorted_items = sorted(
            self._sorted_sets[key].items(),
            key=lambda x: x[1],
            reverse=True,
        )
        sliced = sorted_items[start : stop + 1]
        if withscores:
            return sliced
        return [item[0] for item in sliced]

    # ─────────────────────────────────
    # Hash commands
    # ─────────────────────────────────
    async def hset(self, key: str, field=None, value=None, mapping: dict[str, str] | None = None, **kwargs) -> int:
        """
        Hash HSET

        실제 redis-py는 `hset(key, field, value)` / `hset(key, mapping=...)` 두 형태를 모두 지원한다.
        여기서도 둘 다 처리한다.
        """
        if key not in self._hashes:
            self._hashes[key] = {}
        added = 0
        if field is not None and value is not None:
            if field not in self._hashes[key]:
                added += 1
            self._hashes[key][field] = str(value)
        if mapping:
            for k, v in mapping.items():
                if k not in self._hashes[key]:
                    added += 1
                self._hashes[key][k] = str(v)
        for k, v in kwargs.items():
            if k not in self._hashes[key]:
                added += 1
            self._hashes[key][k] = str(v)
        return added

    async def hgetall(self, key: str) -> dict[str, str]:
        """Hash HGETALL"""
        return dict(self._hashes.get(key, {}))

    async def hget(self, key: str, field: str) -> str | None:
        """Hash HGET"""
        return self._hashes.get(key, {}).get(field)

    async def hdel(self, key: str, *fields: str) -> int:
        """Hash HDEL — 여러 field를 삭제"""
        if key not in self._hashes:
            return 0
        removed = 0
        for f in fields:
            if f in self._hashes[key]:
                del self._hashes[key][f]
                removed += 1
        return removed

    async def hlen(self, key: str) -> int:
        """Hash HLEN"""
        return len(self._hashes.get(key, {}))

    # ─────────────────────────────────
    # Set commands (2026-04-07 추가 — 좋아요 user set 테스트용)
    # ─────────────────────────────────
    async def sadd(self, key: str, *members: str) -> int:
        """Set SADD"""
        if key not in self._sets:
            self._sets[key] = set()
        added = 0
        for m in members:
            if m not in self._sets[key]:
                self._sets[key].add(m)
                added += 1
        return added

    async def srem(self, key: str, *members: str) -> int:
        """Set SREM"""
        if key not in self._sets:
            return 0
        removed = 0
        for m in members:
            if m in self._sets[key]:
                self._sets[key].discard(m)
                removed += 1
        return removed

    async def sismember(self, key: str, member: str) -> int:
        """Set SISMEMBER — 0 또는 1 (redis-py bool 호환)"""
        return 1 if (key in self._sets and member in self._sets[key]) else 0

    async def smembers(self, key: str) -> set[str]:
        """Set SMEMBERS"""
        return set(self._sets.get(key, set()))

    async def scard(self, key: str) -> int:
        """Set SCARD"""
        return len(self._sets.get(key, set()))

    # ─────────────────────────────────
    # Misc
    # ─────────────────────────────────
    async def expire(self, key: str, ttl: int) -> bool:
        """TTL 설정"""
        self._ttls[key] = ttl
        return True

    async def ping(self) -> bool:
        """PING"""
        return True


# ─────────────────────────────────────────
# pytest 픽스처
# ─────────────────────────────────────────

@pytest_asyncio.fixture
async def async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    테스트용 비동기 DB 세션을 생성합니다.

    각 테스트 시작 시 테이블을 생성하고,
    테스트 완료 후 테이블을 삭제합니다.
    """
    # 테이블 생성
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with TestSessionFactory() as session:
        yield session

    # 테이블 삭제 (테스트 격리)
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def fake_redis() -> FakeRedis:
    """테스트용 FakeRedis 인스턴스를 반환합니다."""
    return FakeRedis()


@pytest_asyncio.fixture
async def client(async_session: AsyncSession, fake_redis: FakeRedis) -> AsyncGenerator[AsyncClient, None]:
    """
    FastAPI 테스트 클라이언트를 생성합니다.

    실제 DB/Redis 대신 테스트용 세션과 FakeRedis를 주입합니다.
    user_id는 VARCHAR(50) 문자열로 반환합니다 (DDL 기준).
    """

    # 의존성 오버라이드
    async def override_get_db():
        yield async_session

    async def override_get_redis():
        return fake_redis

    async def override_get_current_user():
        # DDL 기준: user_id VARCHAR(50)
        return TEST_USER_ID

    async def override_get_current_user_optional():
        # DDL 기준: user_id VARCHAR(50)
        return TEST_USER_ID

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis_client] = override_get_redis
    app.dependency_overrides[get_current_user] = override_get_current_user
    app.dependency_overrides[get_current_user_optional] = override_get_current_user_optional

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    # 오버라이드 해제
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """테스트용 JWT 인증 헤더를 반환합니다."""
    token = create_test_token()
    return {"Authorization": f"Bearer {token}"}
