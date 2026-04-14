"""
자동완성 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 AutocompleteService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직(Redis 캐시 우선, MySQL 폴백)은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import json
import logging

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import AutocompleteResponse
from app.search_elasticsearch import ElasticsearchSearchClient
from app.v2.repository.movie_repository import MovieRepository

logger = logging.getLogger(__name__)


class AutocompleteService:
    """검색어 자동완성 서비스 (v2 Raw SQL)"""

    # Redis 캐시 키 접두어
    CACHE_KEY_PREFIX = "autocomplete:v2:"

    def __init__(self, conn: aiomysql.Connection, redis_client: aioredis.Redis):
        """
        Args:
            conn: aiomysql 비동기 커넥션
            redis_client: Redis 비동기 클라이언트
        """
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(conn)
        self._search_es = ElasticsearchSearchClient()

    async def get_suggestions(
        self, prefix: str, limit: int = 10
    ) -> AutocompleteResponse:
        """
        입력 중인 검색어에 대한 자동완성 후보를 반환합니다.

        1단계: Redis 캐시 확인 (TTL 5분)
        2단계: 캐시 미스 → MySQL LIKE 검색
        3단계: 결과를 Redis에 캐싱
        """
        prefix_stripped = prefix.strip()
        if not prefix_stripped:
            return AutocompleteResponse(suggestions=[], did_you_mean=None)

        # 1단계: Redis 캐시 확인
        cache_key = f"{self.CACHE_KEY_PREFIX}{prefix_stripped.lower()}"
        try:
            cached = await self._redis.get(cache_key)
            if cached:
                payload = json.loads(cached)
                suggestions = payload.get("suggestions", []) if isinstance(payload, dict) else payload
                did_you_mean = payload.get("did_you_mean") if isinstance(payload, dict) else None
                logger.debug(f"자동완성 캐시 히트: prefix='{prefix_stripped}', 건수={len(suggestions)}")
                return AutocompleteResponse(
                    suggestions=suggestions[:limit],
                    did_you_mean=did_you_mean,
                )
        except Exception as e:
            logger.warning(f"Redis 자동완성 캐시 조회 실패: {e}")

        # 2단계: Elasticsearch 우선 검색
        es_result = await self._search_es.autocomplete(prefix_stripped, limit)
        if es_result is not None:
            response = AutocompleteResponse(
                suggestions=es_result.suggestions[:limit],
                did_you_mean=es_result.did_you_mean,
            )
        else:
            titles = await self._movie_repo.autocomplete_titles(prefix_stripped, limit)
            response = AutocompleteResponse(suggestions=titles, did_you_mean=None)

        # 3단계: Redis 캐싱
        try:
            await self._redis.setex(
                cache_key,
                self._settings.AUTOCOMPLETE_CACHE_TTL,
                json.dumps(
                    {
                        "suggestions": response.suggestions,
                        "did_you_mean": response.did_you_mean,
                    },
                    ensure_ascii=False,
                ),
            )
        except Exception as e:
            logger.warning(f"Redis 자동완성 캐싱 실패: {e}")

        return response
