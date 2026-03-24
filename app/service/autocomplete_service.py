"""
자동완성 서비스

REQ_031: 검색어 자동완성 (debounce용, 최대 10건)

자동완성 흐름:
1. Redis 캐시 확인 (키: autocomplete:{prefix}, TTL 5분)
2. 캐시 히트 → 즉시 반환
3. 캐시 미스 → MySQL LIKE 검색 (prefix match 우선)
4. 결과를 Redis에 캐싱

성능 최적화:
- Redis 캐시로 DB 부하 최소화 (TTL 5분)
- prefix match 우선: "인터" → "인터스텔라" (인덱스 활용)
- contains match 보충: "스텔라" → "인터스텔라" (부족할 때만)
"""

import json
import logging

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.schema import AutocompleteResponse
from app.repository.movie_repository import MovieRepository

logger = logging.getLogger(__name__)


class AutocompleteService:
    """검색어 자동완성 서비스"""

    # Redis 캐시 키 접두어
    CACHE_KEY_PREFIX = "autocomplete:"

    def __init__(self, session: AsyncSession, redis_client: aioredis.Redis):
        """
        Args:
            session: SQLAlchemy 비동기 세션
            redis_client: Redis 비동기 클라이언트
        """
        self._session = session
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(session)

    async def get_suggestions(
        self, prefix: str, limit: int = 10
    ) -> AutocompleteResponse:
        """
        입력 중인 검색어에 대한 자동완성 후보를 반환합니다.

        클라이언트에서 debounce(300ms)를 적용하여 호출합니다.
        최소 1글자 이상 입력해야 자동완성이 동작합니다.

        Args:
            prefix: 사용자가 입력 중인 키워드
            limit: 최대 반환 건수 (기본 10)

        Returns:
            AutocompleteResponse: 자동완성 후보 목록
        """
        prefix_stripped = prefix.strip()
        if not prefix_stripped:
            return AutocompleteResponse(suggestions=[])

        # ─────────────────────────────────────
        # 1단계: Redis 캐시 확인
        # ─────────────────────────────────────
        cache_key = f"{self.CACHE_KEY_PREFIX}{prefix_stripped.lower()}"
        try:
            cached = await self._redis.get(cache_key)
            if cached:
                # 캐시 히트: JSON 파싱하여 즉시 반환
                suggestions = json.loads(cached)
                logger.debug(f"자동완성 캐시 히트: prefix='{prefix_stripped}', 건수={len(suggestions)}")
                return AutocompleteResponse(suggestions=suggestions[:limit])
        except Exception as e:
            # Redis 장애 시 DB 직접 조회로 폴백
            logger.warning(f"Redis 자동완성 캐시 조회 실패: {e}")

        # ─────────────────────────────────────
        # 2단계: MySQL 검색 (캐시 미스)
        # ─────────────────────────────────────
        titles = await self._movie_repo.autocomplete_titles(prefix_stripped, limit)

        # ─────────────────────────────────────
        # 3단계: Redis 캐싱 (TTL: AUTOCOMPLETE_CACHE_TTL초)
        # ─────────────────────────────────────
        try:
            await self._redis.setex(
                cache_key,
                self._settings.AUTOCOMPLETE_CACHE_TTL,
                json.dumps(titles, ensure_ascii=False),
            )
        except Exception as e:
            # 캐싱 실패는 무시 (다음 요청에서 다시 캐싱)
            logger.warning(f"Redis 자동완성 캐싱 실패: {e}")

        return AutocompleteResponse(suggestions=titles)
