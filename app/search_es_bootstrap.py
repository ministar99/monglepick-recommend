"""
검색용 Elasticsearch 인덱스 bootstrap/backfill 유틸리티.

recommend 서비스만으로도 movies_bm25 인덱스의 검색 필수 자산을 준비할 수 있게 한다.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass

from app.config import get_settings
from app.search_elasticsearch import ESIndexCapabilities

try:
    from elasticsearch import AsyncElasticsearch
except Exception:  # pragma: no cover - import 실패 시 런타임 폴백
    AsyncElasticsearch = None

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SearchESBootstrapResult:
    index: str
    mapping_updated: bool
    capabilities_before: dict
    capabilities_after: dict
    backfill_requested: bool
    reindex_all: bool
    target_docs: int
    updated_docs: int
    version_conflicts: int


class SearchESBootstrapper:
    """검색용 ES 인덱스 매핑/문서 보강을 담당한다."""

    def __init__(self):
        self._settings = get_settings()
        self._client: AsyncElasticsearch | None = None

    def is_available(self) -> bool:
        return bool(
            self._settings.SEARCH_ES_ENABLED
            and self._settings.ELASTICSEARCH_URL
            and self._settings.ELASTICSEARCH_INDEX
            and AsyncElasticsearch is not None
        )

    async def bootstrap(
        self,
        *,
        run_backfill: bool = True,
        reindex_all: bool = False,
    ) -> SearchESBootstrapResult | None:
        if not self.is_available():
            logger.warning(
                "search_es_bootstrap_unavailable",
                extra={
                    "enabled": self._settings.SEARCH_ES_ENABLED,
                    "url": self._settings.ELASTICSEARCH_URL,
                    "index": self._settings.ELASTICSEARCH_INDEX,
                },
            )
            return None

        client = self._get_client()
        index_name = self._settings.ELASTICSEARCH_INDEX

        if not await client.indices.exists(index=index_name):
            logger.warning("search_es_bootstrap_index_missing", extra={"index": index_name})
            return None

        mapping = await client.indices.get_mapping(index=index_name)
        capabilities_before = ESIndexCapabilities.from_mapping(mapping, index_name=index_name)

        mapping_body = self._build_mapping_update(mapping, capabilities_before)
        mapping_updated = bool(mapping_body["properties"])
        if mapping_updated:
            await client.indices.put_mapping(index=index_name, body=mapping_body)
            logger.info(
                "search_es_bootstrap_mapping_updated",
                extra={"index": index_name, "properties": list(mapping_body["properties"].keys())},
            )

        mapping_after = await client.indices.get_mapping(index=index_name)
        capabilities_after = ESIndexCapabilities.from_mapping(mapping_after, index_name=index_name)

        target_docs = 0
        updated_docs = 0
        version_conflicts = 0
        if run_backfill:
            backfill_query = self._build_backfill_query(reindex_all=reindex_all)
            target_docs = await self._count_docs(index_name, backfill_query)
            if target_docs > 0:
                response = await client.update_by_query(
                    index=index_name,
                    conflicts="proceed",
                    refresh=True,
                    wait_for_completion=True,
                    body={
                        "query": backfill_query,
                        "script": self._build_backfill_script(),
                    },
                )
                updated_docs = int(response.get("updated", 0))
                version_conflicts = int(response.get("version_conflicts", 0))
                logger.info(
                    "search_es_bootstrap_backfill_completed",
                    extra={
                        "index": index_name,
                        "target_docs": target_docs,
                        "updated_docs": updated_docs,
                        "version_conflicts": version_conflicts,
                    },
                )

        return SearchESBootstrapResult(
            index=index_name,
            mapping_updated=mapping_updated,
            capabilities_before=asdict(capabilities_before),
            capabilities_after=asdict(capabilities_after),
            backfill_requested=run_backfill,
            reindex_all=reindex_all,
            target_docs=target_docs,
            updated_docs=updated_docs,
            version_conflicts=version_conflicts,
        )

    def _get_client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self._settings.ELASTICSEARCH_URL)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def _count_docs(self, index_name: str, query: dict) -> int:
        response = await self._get_client().count(
            index=index_name,
            body={"query": query},
        )
        return int(response.get("count", 0))

    def _build_mapping_update(
        self,
        mapping_response: dict,
        capabilities: ESIndexCapabilities,
    ) -> dict:
        properties: dict = {}
        if not capabilities.has_title_suggest:
            properties["title_suggest"] = {"type": "completion"}
        if not capabilities.has_title_sort:
            properties["title_sort"] = {"type": "keyword"}
        if not capabilities.has_alternative_titles_korean:
            properties["alternative_titles"] = self._build_alternative_titles_mapping(mapping_response)
        return {"properties": properties}

    def _build_alternative_titles_mapping(self, mapping_response: dict) -> dict:
        index_mapping = {}
        if isinstance(mapping_response, dict) and mapping_response:
            if self._settings.ELASTICSEARCH_INDEX in mapping_response:
                index_mapping = mapping_response.get(self._settings.ELASTICSEARCH_INDEX, {})
            else:
                index_mapping = next(iter(mapping_response.values()), {})

        properties = index_mapping.get("mappings", {}).get("properties", {})
        current = properties.get("alternative_titles", {}) if isinstance(properties, dict) else {}
        field_mapping = {"type": current.get("type", "text")}
        if current.get("analyzer"):
            field_mapping["analyzer"] = current["analyzer"]
        field_mapping["fields"] = {
            "korean": {"type": "text", "analyzer": "korean_analyzer"},
        }
        return field_mapping

    def _build_backfill_query(self, *, reindex_all: bool) -> dict:
        if reindex_all:
            return {"match_all": {}}
        return {
            "bool": {
                "should": [
                    {"bool": {"must_not": {"exists": {"field": "title_sort"}}}},
                    {"bool": {"must_not": {"exists": {"field": "title_suggest"}}}},
                ],
                "minimum_should_match": 1,
            }
        }

    def _build_backfill_script(self) -> dict:
        return {
            "lang": "painless",
            "source": """
                def seen = new HashSet();
                def inputs = new ArrayList();

                if (ctx._source.title != null) {
                    def value = ctx._source.title.toString().trim();
                    if (value.length() > 0) {
                        def normalized = value.toLowerCase();
                        if (!seen.contains(normalized)) {
                            seen.add(normalized);
                            inputs.add(value);
                        }
                    }
                }

                if (ctx._source.title_en != null) {
                    def value = ctx._source.title_en.toString().trim();
                    if (value.length() > 0) {
                        def normalized = value.toLowerCase();
                        if (!seen.contains(normalized)) {
                            seen.add(normalized);
                            inputs.add(value);
                        }
                    }
                }

                if (ctx._source.alternative_titles != null) {
                    for (def item : ctx._source.alternative_titles) {
                        def candidate = null;
                        if (item == null) {
                            candidate = null;
                        } else if (item instanceof Map) {
                            if (item.containsKey('title') && item.title != null) {
                                candidate = item.title.toString();
                            }
                        } else {
                            candidate = item.toString();
                        }

                        if (candidate != null) {
                            def value = candidate.trim();
                            if (value.length() > 0) {
                                def normalized = value.toLowerCase();
                                if (!seen.contains(normalized)) {
                                    seen.add(normalized);
                                    inputs.add(value);
                                }
                            }
                        }
                    }
                }

                ctx._source.title_suggest = ['input': inputs];
                if (ctx._source.title == null) {
                    ctx._source.title_sort = '';
                } else {
                    ctx._source.title_sort = ctx._source.title.toString().trim().toLowerCase();
                }
            """,
        }
