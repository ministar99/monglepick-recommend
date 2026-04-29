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
import re

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.schema import AutocompleteResponse
from app.repository.movie_repository import MovieRepository
from app.search_elasticsearch import ElasticsearchSearchClient

logger = logging.getLogger(__name__)


class AutocompleteService:
    """검색어 자동완성 서비스"""

    # Redis 캐시 키 접두어
    CACHE_KEY_PREFIX = "autocomplete:v2:"
    _KOREAN_TITLE_PATTERN = re.compile(r"[가-힣ㄱ-ㅎㅏ-ㅣ]")

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
        self._search_es = ElasticsearchSearchClient()

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
            return AutocompleteResponse(suggestions=[], did_you_mean=None)

        # ─────────────────────────────────────
        # 1단계: Redis 캐시 확인
        # ─────────────────────────────────────
        cache_key = f"{self.CACHE_KEY_PREFIX}{prefix_stripped.lower()}"
        try:
            cached = await self._redis.get(cache_key)
            if cached:
                # 캐시 히트: JSON 파싱하여 즉시 반환
                payload = json.loads(cached)
                suggestions = payload.get("suggestions", []) if isinstance(payload, dict) else payload
                did_you_mean = payload.get("did_you_mean") if isinstance(payload, dict) else None
                logger.debug(f"자동완성 캐시 히트: prefix='{prefix_stripped}', 건수={len(suggestions)}")
                return AutocompleteResponse(
                    suggestions=self._filter_korean_title_suggestions(suggestions, limit=limit),
                    did_you_mean=self._filter_korean_title(did_you_mean),
                )
        except Exception as e:
            # Redis 장애 시 DB 직접 조회로 폴백
            logger.warning(f"Redis 자동완성 캐시 조회 실패: {e}")

        # ─────────────────────────────────────
        # 2단계: Elasticsearch 우선 검색
        # ─────────────────────────────────────
        es_result = await self._search_es.autocomplete(prefix_stripped, limit)
        if es_result is not None:
            response = AutocompleteResponse(
                suggestions=self._filter_korean_title_suggestions(es_result.suggestions, limit=limit),
                did_you_mean=self._filter_korean_title(es_result.did_you_mean),
            )
        else:
            titles = await self._movie_repo.autocomplete_titles(prefix_stripped, limit)
            response = AutocompleteResponse(
                suggestions=self._filter_korean_title_suggestions(titles, limit=limit),
                did_you_mean=None,
            )

        # ─────────────────────────────────────
        # 3단계: Redis 캐싱 (TTL: AUTOCOMPLETE_CACHE_TTL초)
        # ─────────────────────────────────────
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
            # 캐싱 실패는 무시 (다음 요청에서 다시 캐싱)
            logger.warning(f"Redis 자동완성 캐싱 실패: {e}")

        return response

    def _filter_korean_title_suggestions(self, suggestions: list[str], *, limit: int) -> list[str]:
        filtered: list[str] = []
        for suggestion in suggestions:
            filtered_title = self._filter_korean_title(suggestion)
            if filtered_title is None:
                continue
            filtered.append(filtered_title)
            if len(filtered) >= limit:
                break
        return filtered

    def _filter_korean_title(self, title: str | None) -> str | None:
        if not isinstance(title, str):
            return None

        normalized_title = title.strip()
        if not normalized_title:
            return None

        if self._KOREAN_TITLE_PATTERN.search(normalized_title) is None:
            return None

        return normalized_title
