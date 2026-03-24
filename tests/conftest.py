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
    """

    def __init__(self):
        self._store: dict[str, str] = {}  # String 저장소
        self._sorted_sets: dict[str, dict[str, float]] = {}  # Sorted Set 저장소
        self._hashes: dict[str, dict[str, str]] = {}  # Hash 저장소
        self._ttls: dict[str, int] = {}  # TTL 저장소

    async def get(self, key: str) -> str | None:
        """String GET"""
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        """String SET with TTL"""
        self._store[key] = value
        self._ttls[key] = ttl

    async def delete(self, key: str) -> int:
        """키 삭제"""
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
        return deleted

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
        # score 내림차순 정렬
        sorted_items = sorted(
            self._sorted_sets[key].items(),
            key=lambda x: x[1],
            reverse=True,
        )
        sliced = sorted_items[start : stop + 1]
        if withscores:
            return sliced  # [(member, score), ...]
        return [item[0] for item in sliced]

    async def hset(self, key: str, mapping: dict[str, str] | None = None, **kwargs) -> int:
        """Hash HSET"""
        if key not in self._hashes:
            self._hashes[key] = {}
        if mapping:
            self._hashes[key].update(mapping)
        self._hashes[key].update(kwargs)
        return len(mapping or {}) + len(kwargs)

    async def hgetall(self, key: str) -> dict[str, str]:
        """Hash HGETALL"""
        return self._hashes.get(key, {})

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
