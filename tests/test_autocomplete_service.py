import json
from unittest.mock import AsyncMock

import pytest

from app.search_elasticsearch import ESAutocompleteResult
from app.v2.service.autocomplete_service import AutocompleteService


class FakeRedis:
    def __init__(self, initial_data: dict[str, str] | None = None):
        self._data = initial_data or {}
        self.setex_calls: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._data[key] = value
        self.setex_calls.append((key, ttl, value))


@pytest.mark.asyncio
async def test_get_suggestions_filters_english_only_titles_from_es_results():
    redis = FakeRedis()
    service = AutocompleteService(conn=None, redis_client=redis)
    service._search_es.autocomplete = AsyncMock(
        return_value=ESAutocompleteResult(
            suggestions=["Interstellar", "인터스텔라", "La La Land", "라라랜드", "F1 더 무비"],
            did_you_mean="Parasite",
        )
    )
    service._movie_repo.autocomplete_titles = AsyncMock()

    response = await service.get_suggestions("인", limit=5)

    assert response.suggestions == ["인터스텔라", "라라랜드", "F1 더 무비"]
    assert response.did_you_mean is None


@pytest.mark.asyncio
async def test_get_suggestions_filters_english_only_titles_from_mysql_fallback():
    redis = FakeRedis()
    service = AutocompleteService(conn=None, redis_client=redis)
    service._search_es.autocomplete = AsyncMock(return_value=None)
    service._movie_repo.autocomplete_titles = AsyncMock(
        return_value=["Interstellar", "인터스텔라", "La La Land", "라라랜드"]
    )

    response = await service.get_suggestions("인", limit=5)

    assert response.suggestions == ["인터스텔라", "라라랜드"]
    assert response.did_you_mean is None


@pytest.mark.asyncio
async def test_get_suggestions_filters_english_only_titles_from_cached_payload():
    redis = FakeRedis(
        {
            "autocomplete:v2:in": json.dumps(
                {
                    "suggestions": ["Interstellar", "인터스텔라", "라라랜드"],
                    "did_you_mean": "Parasite",
                },
                ensure_ascii=False,
            )
        }
    )
    service = AutocompleteService(conn=None, redis_client=redis)
    service._search_es.autocomplete = AsyncMock()
    service._movie_repo.autocomplete_titles = AsyncMock()

    response = await service.get_suggestions("in", limit=5)

    assert response.suggestions == ["인터스텔라", "라라랜드"]
    assert response.did_you_mean is None
    service._search_es.autocomplete.assert_not_awaited()
    service._movie_repo.autocomplete_titles.assert_not_awaited()
