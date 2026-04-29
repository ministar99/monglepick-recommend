"""
검색 전용 Elasticsearch 래퍼.

자동완성과 영화 검색에서 사용하며,
예외는 외부로 올리지 않고 서비스 레이어에서 MySQL 폴백 판단에 사용한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from app.config import get_settings

try:
    from elasticsearch import AsyncElasticsearch
except Exception:  # pragma: no cover - import 실패 시 폴백 확인용
    AsyncElasticsearch = None

logger = logging.getLogger(__name__)

SEARCH_TYPE_FIELDS = {
    "all": [
        "title^4",
        "title_en^3",
        "director^2.5",
        "cast^2.5",
        "keywords^1.8",
        "alternative_titles^1.8",
        "alternative_titles.korean^2",
        "overview^1.0",
        "overview_en^0.6",
        "cast_characters^1.5",
    ],
    "title": [
        "title^4",
        "title_en^3",
        "alternative_titles^2",
        "alternative_titles.korean^2",
        "overview^0.8",
    ],
    "director": [
        "director^5",
        "title^1.5",
        "overview^0.4",
    ],
    "actor": [
        "cast^5",
        "cast_characters^3",
        "title^1.5",
    ],
}

EXACT_PHRASE_FIELDS = ("title", "title_en", "director", "cast")
OPTIONAL_FIELD_CAPABILITIES = {
    "alternative_titles.korean": "has_alternative_titles_korean",
}
AUTOCOMPLETE_PREFIX_FIELDS = [
    "title^3",
    "title_en^2.5",
    "alternative_titles^2",
    "alternative_titles.korean^2.2",
]


@dataclass(slots=True)
class ESAutocompleteResult:
    suggestions: list[str]
    did_you_mean: str | None


@dataclass(slots=True)
class ESSearchMovieItem:
    movie_id: str
    title: str
    title_en: str | None
    genres: list[str]
    release_year: int | None
    rating: float | None
    vote_count: int | None
    poster_path: str | None
    trailer_url: str | None
    overview: str | None
    director: str | None = None
    cast: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    collection_name: str | None = None
    matched_queries: list[str] = field(default_factory=list)
    score: float | None = None


@dataclass(slots=True)
class ESSearchMoviesResult:
    movies: list[ESSearchMovieItem]
    total: int
    did_you_mean: str | None
    related_queries: list[str]


@dataclass(slots=True, frozen=True)
class ESIndexCapabilities:
    has_title_suggest: bool = False
    has_title_sort: bool = False
    has_alternative_titles_korean: bool = False

    @classmethod
    def from_mapping(
        cls,
        mapping_response: dict,
        *,
        index_name: str | None = None,
    ) -> "ESIndexCapabilities":
        index_mapping = {}
        if isinstance(mapping_response, dict):
            if index_name and index_name in mapping_response:
                index_mapping = mapping_response.get(index_name, {})
            elif mapping_response:
                index_mapping = next(iter(mapping_response.values()), {})

        properties = index_mapping.get("mappings", {}).get("properties", {})
        if not isinstance(properties, dict):
            properties = {}

        alternative_titles = properties.get("alternative_titles", {})
        alternative_title_fields = (
            alternative_titles.get("fields", {})
            if isinstance(alternative_titles, dict)
            else {}
        )
        if not isinstance(alternative_title_fields, dict):
            alternative_title_fields = {}

        return cls(
            has_title_suggest="title_suggest" in properties,
            has_title_sort="title_sort" in properties,
            has_alternative_titles_korean="korean" in alternative_title_fields,
        )


class ElasticsearchSearchClient:
    """검색용 Elasticsearch 접근을 캡슐화한다."""

    _shared_client: AsyncElasticsearch | None = None
    _shared_client_url: str | None = None
    _shared_capabilities: dict[str, ESIndexCapabilities] = {}

    def __init__(self):
        self._settings = get_settings()

    def is_available(self) -> bool:
        return bool(
            self._settings.SEARCH_ES_ENABLED
            and self._settings.ELASTICSEARCH_URL
            and self._settings.ELASTICSEARCH_INDEX
            and AsyncElasticsearch is not None
        )

    async def autocomplete(self, prefix: str, limit: int = 10) -> ESAutocompleteResult | None:
        if not self.is_available():
            return None

        prefix_cleaned = prefix.strip()
        if not prefix_cleaned:
            return ESAutocompleteResult(suggestions=[], did_you_mean=None)

        try:
            client = self._get_client()
            if not await client.indices.exists(index=self._settings.ELASTICSEARCH_INDEX):
                logger.warning("search_es_index_missing", extra={"index": self._settings.ELASTICSEARCH_INDEX})
                return None
            capabilities = await self._get_index_capabilities(client)

            response = await client.search(
                index=self._settings.ELASTICSEARCH_INDEX,
                body={
                    "size": max(limit * 2, 10),
                    "_source": ["title", "title_en"],
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "match_phrase_prefix": {
                                        "title": {
                                            "query": prefix_cleaned,
                                            "max_expansions": 25,
                                            "boost": 3,
                                        }
                                    }
                                },
                                {
                                    "match_phrase_prefix": {
                                        "title_en": {
                                            "query": prefix_cleaned,
                                            "max_expansions": 25,
                                            "boost": 2.5,
                                        }
                                    }
                                },
                                {
                                    "multi_match": {
                                        "query": prefix_cleaned,
                                        "type": "bool_prefix",
                                        "fields": self._filter_optional_fields(
                                            AUTOCOMPLETE_PREFIX_FIELDS,
                                            capabilities,
                                        ),
                                    }
                                },
                            ],
                            "minimum_should_match": 1,
                        }
                    },
                    "suggest": self._build_suggest_body(prefix_cleaned, capabilities),
                },
            )
        except Exception as exc:
            self._log_es_failure("search_es_autocomplete_failed", exc)
            return None

        did_you_mean, completion_suggestions, phrase_suggestions = self._extract_suggestions(
            response,
            original=prefix_cleaned,
        )
        prefix_hits = self._extract_prefix_hits(response, original=prefix_cleaned)
        suggestions = self._dedupe_suggestions(
            completion_suggestions + prefix_hits + phrase_suggestions,
            original=prefix_cleaned,
            limit=limit,
        )
        return ESAutocompleteResult(suggestions=suggestions, did_you_mean=did_you_mean)

    async def search_movies(
        self,
        *,
        keyword: str | None,
        search_type: str,
        genre: str | None,
        genres: list[str] | None = None,
        genre_match_groups: list[list[str]] | None = None,
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None,
        popularity_max: float | None,
        vote_count_min: int | None,
        sort_by: str,
        sort_order: str,
        page: int,
        size: int,
    ) -> ESSearchMoviesResult | None:
        if not self.is_available():
            return None

        keyword_cleaned = keyword.strip() if isinstance(keyword, str) and keyword.strip() else None
        normalized_genres = [genre_name for genre_name in (genres or []) if genre_name]
        normalized_genre_match_groups = [
            [alias for alias in alias_group if alias]
            for alias_group in (genre_match_groups or [])
            if alias_group
        ]
        is_genre_discovery_search = keyword_cleaned is None and bool(
            normalized_genres or normalized_genre_match_groups
        )

        if keyword_cleaned is None and not is_genre_discovery_search:
            return None

        try:
            client = self._get_client()
            if not await client.indices.exists(index=self._settings.ELASTICSEARCH_INDEX):
                logger.warning("search_es_index_missing", extra={"index": self._settings.ELASTICSEARCH_INDEX})
                return None
            capabilities = await self._get_index_capabilities(client)
            if sort_by == "title" and not capabilities.has_title_sort:
                logger.info(
                    "search_es_title_sort_unavailable",
                    extra={
                        "index": self._settings.ELASTICSEARCH_INDEX,
                        "fallback": "mysql",
                    },
                )
                return None

            response = await client.search(
                index=self._settings.ELASTICSEARCH_INDEX,
                body=self._build_search_body(
                    keyword=keyword_cleaned,
                    search_type=search_type,
                    genre=genre,
                    genres=normalized_genres,
                    genre_match_groups=normalized_genre_match_groups,
                    year_from=year_from,
                    year_to=year_to,
                    rating_min=rating_min,
                    rating_max=rating_max,
                    popularity_min=popularity_min,
                    popularity_max=popularity_max,
                    vote_count_min=vote_count_min,
                    sort_by=sort_by,
                    sort_order=sort_order,
                    page=page,
                    size=size,
                    capabilities=capabilities,
                    prioritize_score=is_genre_discovery_search,
                ),
            )
        except Exception as exc:
            self._log_es_failure("search_es_movie_query_failed", exc)
            return None

        if keyword_cleaned is not None:
            did_you_mean, completion_suggestions, phrase_suggestions = self._extract_suggestions(
                response,
                original=keyword_cleaned,
            )
            prefix_hits = self._extract_prefix_hits(response, original=keyword_cleaned)
            related_queries = self._dedupe_suggestions(
                completion_suggestions + prefix_hits + phrase_suggestions,
                original=keyword_cleaned,
                limit=6,
                exclude={did_you_mean} if did_you_mean else None,
            )
        else:
            did_you_mean = None
            related_queries = []

        hits = response.get("hits", {}).get("hits", [])
        total_raw = response.get("hits", {}).get("total", 0)
        total = total_raw.get("value", 0) if isinstance(total_raw, dict) else int(total_raw or 0)
        movies = [self._to_movie_item(hit) for hit in hits]
        return ESSearchMoviesResult(
            movies=movies,
            total=total,
            did_you_mean=did_you_mean,
            related_queries=related_queries,
        )

    async def search_related_movies(
        self,
        *,
        movie_id: str,
        title: str,
        title_en: str | None,
        overview: str | None,
        director: str | None,
        cast_members: list[str] | None,
        genres: list[str] | None,
        collection_name: str | None,
        limit: int | None = None,
    ) -> list[ESSearchMovieItem] | None:
        """영화 상세용 연관 영화 후보를 Elasticsearch 단일 쿼리로 조회합니다."""
        if not self.is_available():
            return None

        normalized_limit = limit or self._settings.RELATED_MOVIES_LIMIT
        source_title = title.strip()
        if not source_title:
            return []

        try:
            client = self._get_client()
            if not await client.indices.exists(index=self._settings.ELASTICSEARCH_INDEX):
                logger.warning("search_es_index_missing", extra={"index": self._settings.ELASTICSEARCH_INDEX})
                return None

            response = await client.search(
                index=self._settings.ELASTICSEARCH_INDEX,
                body=self._build_related_movie_search_body(
                    movie_id=movie_id,
                    title=source_title,
                    title_en=title_en,
                    overview=overview,
                    director=director,
                    cast_members=cast_members or [],
                    genres=genres or [],
                    collection_name=collection_name,
                    limit=normalized_limit,
                ),
            )
        except Exception as exc:
            self._log_es_failure("search_es_related_movie_query_failed", exc)
            return None

        hits = response.get("hits", {}).get("hits", [])
        related_movies: list[ESSearchMovieItem] = []
        seen_ids: set[str] = set()

        for hit in hits:
            movie = self._to_movie_item(hit)
            if not movie.movie_id or movie.movie_id == movie_id or movie.movie_id in seen_ids:
                continue
            seen_ids.add(movie.movie_id)
            related_movies.append(movie)
            if len(related_movies) >= normalized_limit:
                break

        return related_movies

    async def search_collection_movies(
        self,
        *,
        movie_id: str,
        collection_name: str | None,
        page_size: int = 100,
    ) -> list[ESSearchMovieItem] | None:
        """같은 collection_name 을 가진 영화를 모두 조회합니다."""
        if not self.is_available():
            return None

        normalized_collection_name = collection_name.strip() if isinstance(collection_name, str) else ""
        if not normalized_collection_name:
            return []

        try:
            client = self._get_client()
            if not await client.indices.exists(index=self._settings.ELASTICSEARCH_INDEX):
                logger.warning("search_es_index_missing", extra={"index": self._settings.ELASTICSEARCH_INDEX})
                return None

            collection_movies: list[ESSearchMovieItem] = []
            seen_ids: set[str] = set()
            page = 0

            while True:
                response = await client.search(
                    index=self._settings.ELASTICSEARCH_INDEX,
                    body=self._build_collection_movie_search_body(
                        movie_id=movie_id,
                        collection_name=normalized_collection_name,
                        page=page,
                        page_size=page_size,
                    ),
                )
                hits = response.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    movie = self._to_movie_item(hit)
                    if not movie.movie_id or movie.movie_id == movie_id or movie.movie_id in seen_ids:
                        continue
                    seen_ids.add(movie.movie_id)
                    collection_movies.append(movie)

                if len(hits) < page_size:
                    break
                page += 1

            return collection_movies
        except Exception as exc:
            self._log_es_failure("search_es_collection_movie_query_failed", exc)
            return None

    def _build_search_body(
        self,
        *,
        keyword: str | None,
        search_type: str,
        genre: str | None,
        genres: list[str] | None,
        genre_match_groups: list[list[str]] | None,
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None,
        popularity_max: float | None,
        vote_count_min: int | None,
        sort_by: str,
        sort_order: str,
        page: int,
        size: int,
        capabilities: ESIndexCapabilities,
        prioritize_score: bool,
    ) -> dict:
        query = (
            self._build_movie_query(
                keyword=keyword,
                search_type=search_type,
                genre=genre,
                year_from=year_from,
                year_to=year_to,
                rating_min=rating_min,
                rating_max=rating_max,
                popularity_min=popularity_min,
                popularity_max=popularity_max,
                vote_count_min=vote_count_min,
                capabilities=capabilities,
            )
            if keyword is not None
            else self._build_genre_discovery_query(
                genres=genres or [],
                genre_match_groups=genre_match_groups or [],
                year_from=year_from,
                year_to=year_to,
                rating_min=rating_min,
                rating_max=rating_max,
                popularity_min=popularity_min,
                popularity_max=popularity_max,
                vote_count_min=vote_count_min,
            )
        )
        body = {
            "from": (page - 1) * size,
            "size": size,
            "query": query,
            "sort": self._build_sort(
                sort_by=sort_by,
                sort_order=sort_order,
                capabilities=capabilities,
                prioritize_score=prioritize_score,
            ),
        }
        if keyword is not None:
            body["suggest"] = self._build_suggest_body(keyword, capabilities)
        return body

    def _build_related_movie_search_body(
        self,
        *,
        movie_id: str,
        title: str,
        title_en: str | None,
        overview: str | None,
        director: str | None,
        cast_members: list[str],
        genres: list[str],
        collection_name: str | None,
        limit: int,
    ) -> dict:
        like_texts = self._build_related_like_texts(
            title=title,
            title_en=title_en,
            overview=overview,
        )
        should_clauses: list[dict] = []

        if like_texts:
            should_clauses.append(
                {
                    "more_like_this": {
                        "fields": [
                            "title",
                            "title_en",
                            "alternative_titles",
                            "overview",
                            "overview_en",
                            "keywords",
                        ],
                        "like": like_texts,
                        "min_term_freq": 1,
                        "min_doc_freq": 1,
                        "max_query_terms": 30,
                        "boost": 5.2,
                    }
                }
            )

        if director and director.strip():
            should_clauses.append(
                {
                    "match_phrase": {
                        "director": {
                            "query": director.strip(),
                            "boost": 1.1,
                        }
                    }
                }
            )

        normalized_genres = [genre for genre in dict.fromkeys(genres) if genre]
        if normalized_genres:
            should_clauses.extend(
                {
                    "constant_score": {
                        "filter": {"term": {"genres": genre_name}},
                        "boost": 2.8,
                    }
                }
                for genre_name in normalized_genres[:4]
            )

        normalized_cast = [member for member in dict.fromkeys(cast_members) if member]
        should_clauses.extend(
            {
                "match_phrase": {
                    "cast": {
                        "query": actor_name,
                        "boost": 1.3,
                    }
                }
            }
            for actor_name in normalized_cast[:3]
        )

        if collection_name and collection_name.strip():
            should_clauses.append(
                {
                    "match_phrase": {
                        "collection_name": {
                            "query": collection_name.strip(),
                            "boost": 5.6,
                        }
                    }
                }
            )

        if not should_clauses:
            should_clauses.append(
                {
                    "multi_match": {
                        "query": title,
                        "fields": ["title^3", "title_en^2", "overview"],
                        "type": "best_fields",
                        "fuzziness": "AUTO",
                    }
                }
            )

        return {
            "size": max(limit * 2, 30),
            "_source": [
                "id",
                "movie_id",
                "title",
                "title_en",
                "genres",
                "release_year",
                "rating",
                "vote_count",
                "poster_path",
                "trailer_url",
                "overview",
                "director",
                "cast",
                "keywords",
                "collection_name",
            ],
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {"id": movie_id}},
                    ],
                    "filter": self._build_common_filters(
                        genre=None,
                        year_from=None,
                        year_to=None,
                        rating_min=None,
                        rating_max=None,
                        popularity_min=None,
                        popularity_max=None,
                        vote_count_min=None,
                    ),
                    "should": should_clauses,
                    "minimum_should_match": 1,
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"rating": {"order": "desc", "missing": "_last"}},
                {"vote_count": {"order": "desc", "missing": "_last"}},
                {"release_year": {"order": "desc", "missing": "_last"}},
            ],
        }

    def _build_collection_movie_search_body(
        self,
        *,
        movie_id: str,
        collection_name: str,
        page: int,
        page_size: int,
    ) -> dict:
        return {
            "from": page * page_size,
            "size": page_size,
            "_source": [
                "id",
                "movie_id",
                "title",
                "title_en",
                "genres",
                "release_year",
                "rating",
                "vote_count",
                "poster_path",
                "trailer_url",
                "overview",
                "director",
                "cast",
                "keywords",
                "collection_name",
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "collection_name": {
                                    "query": collection_name,
                                }
                            }
                        }
                    ],
                    "must_not": [
                        {"term": {"id": movie_id}},
                    ],
                    "filter": self._build_common_filters(
                        genre=None,
                        year_from=None,
                        year_to=None,
                        rating_min=None,
                        rating_max=None,
                        popularity_min=None,
                        popularity_max=None,
                        vote_count_min=None,
                    ),
                }
            },
            "sort": [
                {"release_year": {"order": "asc", "missing": "_last"}},
                {"vote_count": {"order": "desc", "missing": "_last"}},
                {"rating": {"order": "desc", "missing": "_last"}},
                {"_score": {"order": "desc"}},
            ],
        }

    def _build_related_like_texts(
        self,
        *,
        title: str,
        title_en: str | None,
        overview: str | None,
    ) -> list[str]:
        like_texts = [title.strip()]
        if title_en and title_en.strip():
            like_texts.append(title_en.strip())
        if overview and overview.strip():
            like_texts.append(overview.strip()[:1200])
        return like_texts

    def _get_client(self) -> AsyncElasticsearch:
        cls = type(self)
        if cls._shared_client is None:
            cls._shared_client = AsyncElasticsearch(self._settings.ELASTICSEARCH_URL)
            cls._shared_client_url = self._settings.ELASTICSEARCH_URL
        return cls._shared_client

    @classmethod
    async def close_shared_client(cls) -> None:
        if cls._shared_client is not None:
            await cls._shared_client.close()
            cls._shared_client = None
            cls._shared_client_url = None
        cls._shared_capabilities.clear()

    async def _get_index_capabilities(
        self,
        client: AsyncElasticsearch,
    ) -> ESIndexCapabilities:
        cached = type(self)._shared_capabilities.get(self._settings.ELASTICSEARCH_INDEX)
        if cached is not None:
            return cached

        try:
            mapping = await client.indices.get_mapping(index=self._settings.ELASTICSEARCH_INDEX)
            capabilities = ESIndexCapabilities.from_mapping(
                mapping,
                index_name=self._settings.ELASTICSEARCH_INDEX,
            )
            type(self)._shared_capabilities[self._settings.ELASTICSEARCH_INDEX] = capabilities
            logger.info(
                "search_es_capabilities_loaded",
                extra={
                    "index": self._settings.ELASTICSEARCH_INDEX,
                    "has_title_suggest": capabilities.has_title_suggest,
                    "has_title_sort": capabilities.has_title_sort,
                    "has_alternative_titles_korean": capabilities.has_alternative_titles_korean,
                },
            )
        except Exception as exc:
            self._log_es_failure("search_es_capabilities_load_failed", exc)
            capabilities = ESIndexCapabilities()
            type(self)._shared_capabilities[self._settings.ELASTICSEARCH_INDEX] = capabilities

        return capabilities

    def _log_es_failure(self, event: str, exc: Exception) -> None:
        error_message = str(exc)
        extra = {"error": error_message}
        if "aiohttp" in error_message.lower():
            extra["action"] = "Install aiohttp in monglepick-recommend environment"
        logger.warning(event, extra=extra)

    def _filter_optional_fields(
        self,
        fields: list[str],
        capabilities: ESIndexCapabilities,
    ) -> list[str]:
        filtered: list[str] = []
        for field in fields:
            field_name = field.split("^", 1)[0]
            capability_name = OPTIONAL_FIELD_CAPABILITIES.get(field_name)
            if capability_name and not getattr(capabilities, capability_name, False):
                continue
            filtered.append(field)
        return filtered

    def _build_movie_query(
        self,
        *,
        keyword: str,
        search_type: str,
        genre: str | None,
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None = None,
        popularity_max: float | None = None,
        vote_count_min: int | None,
        capabilities: ESIndexCapabilities,
    ) -> dict:
        fields = self._filter_optional_fields(
            SEARCH_TYPE_FIELDS.get(search_type, SEARCH_TYPE_FIELDS["title"]),
            capabilities,
        )
        filters = self._build_common_filters(
            genre=genre,
            year_from=year_from,
            year_to=year_to,
            rating_min=rating_min,
            rating_max=rating_max,
            popularity_min=popularity_min,
            popularity_max=popularity_max,
            vote_count_min=vote_count_min,
        )

        return {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": keyword,
                            "fields": fields,
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                            "max_expansions": 25,
                            "tie_breaker": 0.3,
                        }
                    }
                ],
                "filter": filters,
                "should": [
                    {"match_phrase": {field: {"query": keyword, "boost": 4.0}}}
                    for field in EXACT_PHRASE_FIELDS
                ],
            }
        }

    def _build_genre_discovery_query(
        self,
        *,
        genres: list[str],
        genre_match_groups: list[list[str]],
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None,
        popularity_max: float | None,
        vote_count_min: int | None,
    ) -> dict:
        should_clauses = self._build_genre_score_clauses(
            genres=genres,
            genre_match_groups=genre_match_groups,
        )
        return {
            "bool": {
                "filter": self._build_common_filters(
                    genre=None,
                    year_from=year_from,
                    year_to=year_to,
                    rating_min=rating_min,
                    rating_max=rating_max,
                    popularity_min=popularity_min,
                    popularity_max=popularity_max,
                    vote_count_min=vote_count_min,
                ),
                "should": should_clauses,
                "minimum_should_match": 1,
            }
        }

    def _build_genre_score_clauses(
        self,
        *,
        genres: list[str],
        genre_match_groups: list[list[str]],
    ) -> list[dict]:
        score_clauses: list[dict] = []

        if genre_match_groups:
            for alias_group in genre_match_groups:
                unique_aliases = [alias for alias in dict.fromkeys(alias_group) if alias]
                if not unique_aliases:
                    continue
                score_clauses.append(
                    {
                        "constant_score": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {"term": {"genres": alias}}
                                        for alias in unique_aliases
                                    ],
                                    "minimum_should_match": 1,
                                }
                            },
                            "boost": 1.0,
                        }
                    }
                )

        if score_clauses:
            return score_clauses

        return [
            {
                "constant_score": {
                    "filter": {"term": {"genres": genre_name}},
                    "boost": 1.0,
                }
            }
            for genre_name in dict.fromkeys(genres)
            if genre_name
        ]

    def _build_common_filters(
        self,
        *,
        genre: str | None,
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None = None,
        popularity_max: float | None = None,
        vote_count_min: int | None,
    ) -> list[dict]:
        filters: list[dict] = [
            {"bool": {"must_not": {"term": {"adult": True}}}},
            {
                "bool": {
                    "must_not": {
                        "terms": {
                            "certification": [
                                "청소년 관람 불가",
                                "19세관람가(청소년관람불가)",
                            ]
                        }
                    }
                }
            },
            {"bool": {"must_not": {"term": {"genres": "에로"}}}},
        ]

        if genre:
            filters.append({"term": {"genres": genre}})
        if year_from is not None:
            filters.append({"range": {"release_year": {"gte": year_from}}})
        if year_to is not None:
            filters.append({"range": {"release_year": {"lte": year_to}}})
        if rating_min is not None:
            filters.append({"range": {"rating": {"gte": rating_min}}})
        if rating_max is not None:
            filters.append({"range": {"rating": {"lte": rating_max}}})
        if popularity_min is not None:
            filters.append({"range": {"popularity_score": {"gte": popularity_min}}})
        if popularity_max is not None:
            filters.append({"range": {"popularity_score": {"lte": popularity_max}}})
        if vote_count_min is not None:
            filters.append({"range": {"vote_count": {"gte": vote_count_min}}})

        return filters

    def _build_sort(
        self,
        *,
        sort_by: str,
        sort_order: str,
        capabilities: ESIndexCapabilities,
        prioritize_score: bool = False,
    ) -> list[dict]:
        direction = "asc" if sort_order == "asc" else "desc"
        if sort_by == "rating":
            sort = [
                {"rating": {"order": direction, "missing": "_last"}},
                {"vote_count": {"order": "desc", "missing": "_last"}},
                {"_score": {"order": "desc"}},
            ]
            if prioritize_score:
                return [{"_score": {"order": "desc"}}] + sort[:-1]
            return sort
        if sort_by == "release_date":
            sort = [
                {"release_year": {"order": direction, "missing": "_last"}},
                {"_score": {"order": "desc"}},
            ]
            if prioritize_score:
                return [{"_score": {"order": "desc"}}] + sort[:-1]
            return sort
        if sort_by == "title":
            if not capabilities.has_title_sort:
                return []
            sort = [
                {"title_sort": {"order": direction, "missing": "_last"}},
                {"release_year": {"order": "desc", "missing": "_last"}},
                {"_score": {"order": "desc"}},
            ]
            if prioritize_score:
                return [{"_score": {"order": "desc"}}] + sort[:-1]
            return sort
        return [
            {"_score": {"order": "desc"}},
            {"rating": {"order": "desc", "missing": "_last"}},
            {"vote_count": {"order": "desc", "missing": "_last"}},
            {"release_year": {"order": "desc", "missing": "_last"}},
        ]

    def _build_suggest_body(
        self,
        text: str,
        capabilities: ESIndexCapabilities,
    ) -> dict:
        suggest = {
            "title_phrase_ko": {
                "text": text,
                "phrase": {
                    "field": "title",
                    "size": 3,
                    "gram_size": 1,
                    "direct_generator": [{"field": "title", "suggest_mode": "always"}],
                    "highlight": {"pre_tag": "", "post_tag": ""},
                },
            },
            "title_phrase_en": {
                "text": text,
                "phrase": {
                    "field": "title_en",
                    "size": 3,
                    "gram_size": 1,
                    "direct_generator": [{"field": "title_en", "suggest_mode": "always"}],
                    "highlight": {"pre_tag": "", "post_tag": ""},
                },
            },
        }
        if capabilities.has_title_suggest:
            suggest["title_completion"] = {
                "prefix": text,
                "completion": {
                    "field": "title_suggest",
                    "size": 10,
                    "skip_duplicates": True,
                },
            }
        return suggest

    def _extract_suggestions(
        self,
        response: dict,
        *,
        original: str,
    ) -> tuple[str | None, list[str], list[str]]:
        suggest = response.get("suggest", {})
        completion_suggestions = self._parse_options(suggest.get("title_completion", []))
        phrase_suggestions = self._parse_options(suggest.get("title_phrase_ko", []))
        phrase_suggestions += self._parse_options(suggest.get("title_phrase_en", []))
        phrase_suggestions = self._dedupe_suggestions(phrase_suggestions, original=original, limit=5)
        did_you_mean = phrase_suggestions[0] if phrase_suggestions else None
        return did_you_mean, completion_suggestions, phrase_suggestions

    def _extract_prefix_hits(self, response: dict, *, original: str) -> list[str]:
        suggestions: list[str] = []
        for hit in response.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            suggestions.extend(
                value
                for value in (source.get("title"), source.get("title_en"))
                if isinstance(value, str) and value.strip()
            )
        return self._dedupe_suggestions(suggestions, original=original, limit=8)

    def _parse_options(self, entries: list[dict]) -> list[str]:
        parsed: list[str] = []
        for entry in entries:
            for option in entry.get("options", []):
                text = option.get("text")
                if isinstance(text, str) and text.strip():
                    parsed.append(text.strip())
                source = option.get("_source")
                if isinstance(source, dict):
                    for key in ("title", "title_en"):
                        value = source.get(key)
                        if isinstance(value, str) and value.strip():
                            parsed.append(value.strip())
        return parsed

    def _dedupe_suggestions(
        self,
        suggestions: list[str],
        *,
        original: str,
        limit: int,
        exclude: set[str | None] | None = None,
    ) -> list[str]:
        excluded = {
            item.strip().lower()
            for item in (exclude or set())
            if isinstance(item, str) and item.strip()
        }
        original_normalized = original.strip().lower()
        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in suggestions:
            if not isinstance(candidate, str):
                continue
            text = candidate.strip()
            normalized = text.lower()
            if not text or normalized == original_normalized or normalized in seen or normalized in excluded:
                continue
            seen.add(normalized)
            deduped.append(text)
            if len(deduped) >= limit:
                break
        return deduped

    def _to_movie_item(self, hit: dict) -> ESSearchMovieItem:
        source = hit.get("_source", {})
        genres = source.get("genres")
        if not isinstance(genres, list):
            genres = self._coerce_string_list(genres)

        return ESSearchMovieItem(
            movie_id=str(source.get("id") or source.get("movie_id") or ""),
            title=source.get("title", "") or "",
            title_en=source.get("title_en"),
            genres=[genre for genre in genres if isinstance(genre, str)],
            release_year=source.get("release_year"),
            rating=source.get("rating"),
            vote_count=source.get("vote_count"),
            poster_path=source.get("poster_path"),
            trailer_url=source.get("trailer_url"),
            overview=source.get("overview"),
            director=source.get("director"),
            cast=self._coerce_string_list(source.get("cast")),
            keywords=self._coerce_string_list(source.get("keywords")),
            collection_name=source.get("collection_name"),
            matched_queries=[
                matched_query
                for matched_query in (hit.get("matched_queries") or [])
                if isinstance(matched_query, str) and matched_query.strip()
            ],
            score=hit.get("_score"),
        )

    def _coerce_string_list(self, value: object) -> list[str]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, str) and item.strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return []
