"""
비동기 데이터베이스 세션 팩토리

SQLAlchemy 2.0 비동기 엔진 + AsyncSession을 사용합니다.
Spring Boot 백엔드와 동일한 MySQL 인스턴스에 접속하며,
커넥션 풀은 최대 20개, 유휴 최소 5개로 설정합니다.

사용법:
    async with get_async_session() as session:
        result = await session.execute(...)
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings

# ─────────────────────────────────────────
# SQLAlchemy 비동기 엔진 생성
# ─────────────────────────────────────────
# echo: settings.SQL_ECHO 플래그 로 제어 (개발/디버깅 시 .env SQL_ECHO=true)
#   - True  → 실행된 SQL 과 바인딩 파라미터가 sqlalchemy.engine 로거로 INFO 출력
#   - False → 로그 비활성 (운영 기본값)
# pool_size=20: Spring Boot HikariCP maximum-pool-size와 동일
# pool_recycle=1800: 30분마다 커넥션 재생성 (MySQL wait_timeout 대응)
_settings = get_settings()
engine = create_async_engine(
    _settings.database_url,
    echo=_settings.SQL_ECHO,
    pool_size=20,
    max_overflow=10,
    pool_recycle=1800,
    pool_pre_ping=True,  # 커넥션 유효성 사전 검사
)

# 비동기 세션 팩토리
# expire_on_commit=False: 커밋 후에도 객체 속성에 접근 가능
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    """
    SQLAlchemy ORM 모델의 기본 클래스

    모든 엔티티 모델은 이 클래스를 상속받습니다.
    Spring Boot JPA 엔티티와 동일한 테이블에 매핑됩니다.
    """
    pass


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    비동기 DB 세션을 생성하고 요청 완료 후 자동으로 닫습니다.

    FastAPI Depends()에서 사용하는 제너레이터 함수입니다.
    세션 스코프는 하나의 HTTP 요청과 동일합니다.

    Yields:
        AsyncSession: SQLAlchemy 비동기 세션
    """
    async with async_session_factory() as session:
        try:
            yield session
            # 정상 완료 시 커밋
            await session.commit()
        except Exception:
            # 예외 발생 시 롤백
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """
    데이터베이스 테이블을 생성합니다.

    search_history, trending_keywords 등
    이 서비스가 소유하는 테이블만 생성합니다.
    movies, users, user_preferences는 Spring Boot가 관리합니다.
    """
    async with engine.begin() as conn:
        # 이 서비스 소유 테이블만 생성 (기존 테이블은 건드리지 않음)
        await conn.run_sync(Base.metadata.create_all)


async def close_db() -> None:
    """엔진 종료 시 커넥션 풀을 정리합니다."""
    await engine.dispose()
