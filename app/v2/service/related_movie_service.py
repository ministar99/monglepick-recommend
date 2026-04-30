"""
영화 상세 연관 영화 서비스.

실시간 경로는 다음 순서로 동작한다.
1. 같은 컬렉션 영화만 먼저 빠르게 조회
2. Qdrant 줄거리 벡터 유사 후보로 나머지 슬롯을 채움
3. 완성된 결과를 Redis에 캐시

Neo4j는 실시간 경로에서 제외하고, Elasticsearch는 최후의 폴백으로만 사용한다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

import aiomysql
import httpx
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import RelatedMovieItem, RelatedMoviesResponse
from app.search_elasticsearch import ESSearchMovieItem, ElasticsearchSearchClient
from app.v2.model.dto import MovieDTO
from app.v2.repository.movie_repository import MovieRepository
from app.v2.service.poster_policy import (
    build_tmdb_poster_url,
    collect_exact_title_candidates,
    is_valid_internal_poster_path,
)

logger = logging.getLogger(__name__)

_NEO4J_DIRECT_RELATION_QUERY = """
MATCH (source:Movie)-[r:SIMILAR_TO|RECOMMENDED]->(m:Movie)
WHERE source.id IN $source_ids AND NOT m.id IN $source_ids
RETURN m.id AS candidate_id,
       collect(DISTINCT type(r)) AS relation_types,
       COALESCE(m.rating, 0.0) AS rating
ORDER BY rating DESC
LIMIT $limit
"""

_NEO4J_COLLECTION_QUERY = """
MATCH (source:Movie)-[:PART_OF_COLLECTION]->(c:Collection)<-[:PART_OF_COLLECTION]-(m:Movie)
WHERE source.id IN $source_ids AND NOT m.id IN $source_ids
RETURN m.id AS candidate_id,
       c.name AS collection_name,
       COALESCE(m.rating, 0.0) AS rating
ORDER BY rating DESC
LIMIT $limit
"""

_NEO4J_DIRECTOR_QUERY = """
MATCH (:Person {name: $director})-[:DIRECTED]->(m:Movie)
WHERE NOT m.id IN $source_ids
RETURN m.id AS candidate_id,
       COALESCE(m.rating, 0.0) AS rating
ORDER BY rating DESC
LIMIT $limit
"""

_NEO4J_CAST_QUERY = """
UNWIND $actors AS actor
MATCH (:Person {name: actor})-[:ACTED_IN]->(m:Movie)
WHERE NOT m.id IN $source_ids
RETURN m.id AS candidate_id,
       collect(DISTINCT actor) AS matched_actors,
       count(DISTINCT actor) AS actor_count,
       COALESCE(m.rating, 0.0) AS rating
ORDER BY actor_count DESC, rating DESC
LIMIT $limit
"""


class RelatedMovieNotFoundError(LookupError):
    """연관 영화 조회의 기준 영화를 찾지 못한 경우의 예외입니다."""


@dataclass
class RelatedCandidate:
    """외부 후보를 MySQL 영화 행으로 합치기 전 임시 누적 구조입니다."""

    score: float = 0.0
    qdrant_vector_similarity: float = 0.0
    qdrant_vector_rank: int | None = None
    reasons: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    def merge(
        self,
        *,
        score: float,
        reason: str | None,
        source: str,
        qdrant_vector_similarity: float | None = None,
        qdrant_vector_rank: int | None = None,
    ) -> None:
        """점수/사유/소스를 중복 없이 누적합니다."""
        self.score += score
        if qdrant_vector_similarity is not None:
            self.qdrant_vector_similarity = max(self.qdrant_vector_similarity, qdrant_vector_similarity)
        if qdrant_vector_rank is not None:
            if self.qdrant_vector_rank is None:
                self.qdrant_vector_rank = qdrant_vector_rank
            else:
                self.qdrant_vector_rank = min(self.qdrant_vector_rank, qdrant_vector_rank)
        if reason and reason not in self.reasons:
            self.reasons.append(reason)
        if source not in self.sources:
            self.sources.append(source)


class RelatedMovieService:
    """컬렉션 우선 + Qdrant 기반 연관 영화 서비스입니다."""

    _CACHE_PREFIX = "related:movies"
    _CACHE_VERSION = "v6"
    _TITLE_LOOKUP_WINDOW_MULTIPLIER = 3
    _TITLE_LOOKUP_MIN_CANDIDATES = 30
    _TITLE_LOOKUP_MAX_CANDIDATES = 60

    def __init__(
        self,
        conn: aiomysql.Connection,
        redis_client: aioredis.Redis | None = None,
    ):
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(conn)
        self._search_es = ElasticsearchSearchClient()

    @classmethod
    def _cache_key(cls, movie_id: str, limit: int) -> str:
        """연관 영화 최종 응답 캐시 키를 생성합니다."""
        return f"{cls._CACHE_PREFIX}:{cls._CACHE_VERSION}:{movie_id}:limit:{limit}"

    async def get_collection_related_movies(self, movie_id: str) -> RelatedMoviesResponse:
        """같은 컬렉션 영화만 빠르게 반환합니다."""
        movie = await self._movie_repo.find_by_id(movie_id)
        if movie is None:
            raise RelatedMovieNotFoundError(f"영화 ID '{movie_id}'를 찾을 수 없습니다.")

        try:
            collection_movies_from_es = await self._search_es.search_collection_movies(
                movie_id=movie.movie_id,
                collection_name=movie.collection_name,
            )
        except Exception as exc:
            logger.warning("related_collection_es_fetch_failed movie_id=%s error=%s", movie.movie_id, exc)
            collection_movies_from_es = None

        if collection_movies_from_es is not None:
            title_lookup = await self._build_title_lookup(es_movies=collection_movies_from_es)
            related_items = self._build_collection_es_movie_items(
                collection_movies_from_es,
                title_lookup=title_lookup,
            )
            return RelatedMoviesResponse(movies=related_items)

        try:
            collection_movies = (
                await self._movie_repo.find_by_collection_name(
                    movie.collection_name,
                    exclude_movie_id=movie.movie_id,
                )
                if movie.collection_name
                else []
            )
        except Exception as exc:
            logger.warning("related_collection_fetch_failed movie_id=%s error=%s", movie.movie_id, exc)
            collection_movies = []
        title_lookup = await self._build_title_lookup(dto_movies=collection_movies)
        related_items = self._build_collection_movie_items(
            collection_movies,
            title_lookup=title_lookup,
        )
        return RelatedMoviesResponse(movies=related_items)

    async def get_related_movies(
        self,
        movie_id: str,
        limit: int | None = None,
    ) -> RelatedMoviesResponse:
        """
        특정 영화의 연관 영화 목록을 반환합니다.

        Args:
            movie_id: 기준 영화 ID
            limit: 최대 반환 건수

        Returns:
            RelatedMoviesResponse
        """
        normalized_limit = limit or self._settings.RELATED_MOVIES_LIMIT
        cache_key = self._cache_key(movie_id, normalized_limit)

        cached_response = await self._read_cached_related_movies(cache_key)
        if cached_response is not None:
            return cached_response

        movie = await self._movie_repo.find_by_id(movie_id)
        if movie is None:
            raise RelatedMovieNotFoundError(f"영화 ID '{movie_id}'를 찾을 수 없습니다.")

        candidate_map: dict[str, RelatedCandidate] = {}
        try:
            collection_movies = (
                await self._movie_repo.find_by_collection_name(
                    movie.collection_name,
                    exclude_movie_id=movie.movie_id,
                )
                if movie.collection_name
                else []
            )
        except Exception as exc:
            logger.warning("related_movies_collection_fetch_failed movie_id=%s error=%s", movie.movie_id, exc)
            collection_movies = []
        qdrant_fetch_succeeded = False
        try:
            qdrant_result = await self._fetch_qdrant_candidates(
                movie,
                limit=max(normalized_limit * 3, 40) if movie.collection_name else max(normalized_limit * 2, 20),
            )
            qdrant_fetch_succeeded = True
            self._merge_candidate_maps(candidate_map, qdrant_result)
        except Exception as exc:
            logger.warning("related_movies_qdrant_failed movie_id=%s error=%s", movie.movie_id, exc)

        candidate_movies = await self._movie_repo.find_by_identifiers(list(candidate_map.keys())) if candidate_map else []
        title_lookup = await self._build_title_lookup(
            dto_movies=self._select_title_lookup_dto_movies(
                collection_movies=collection_movies,
                candidate_map=candidate_map,
                candidate_movies=candidate_movies,
                limit=normalized_limit,
            ),
        )
        related_items = self._build_related_movie_items(
            source_movie=movie,
            collection_movies=collection_movies,
            candidate_map=candidate_map,
            candidate_movies=candidate_movies,
            limit=normalized_limit,
            title_lookup=title_lookup,
        )
        if related_items:
            response = RelatedMoviesResponse(movies=related_items)
            if qdrant_fetch_succeeded:
                await self._write_cached_related_movies(cache_key, response)
            return response

        es_related_items = await self._fetch_es_related_movies(movie, limit=normalized_limit)
        response = RelatedMoviesResponse(movies=es_related_items)
        if qdrant_fetch_succeeded:
            await self._write_cached_related_movies(cache_key, response)
        return response

    async def _read_cached_related_movies(
        self,
        cache_key: str,
    ) -> RelatedMoviesResponse | None:
        """Redis 캐시 hit 시 직렬화된 연관 영화 응답을 복원합니다."""
        if self._redis is None:
            return None

        try:
            cached = await self._redis.get(cache_key)
            if not cached:
                return None
            return RelatedMoviesResponse.model_validate_json(cached)
        except Exception as exc:
            logger.warning("related_movies_cache_read_error key=%s error=%s", cache_key, exc)
            return None

    async def _write_cached_related_movies(
        self,
        cache_key: str,
        response: RelatedMoviesResponse,
    ) -> None:
        """최종 연관 영화 응답을 Redis에 best-effort로 저장합니다."""
        if self._redis is None:
            return

        try:
            await self._redis.setex(
                cache_key,
                self._settings.RELATED_MOVIES_CACHE_TTL,
                response.model_dump_json(),
            )
        except Exception as exc:
            logger.warning("related_movies_cache_write_error key=%s error=%s", cache_key, exc)

    async def _fetch_es_related_movies(
        self,
        movie: MovieDTO,
        limit: int,
    ) -> list[RelatedMovieItem]:
        """Elasticsearch 단일 검색으로 연관 영화를 빠르게 조회합니다."""
        collection_movies, es_movies = await asyncio.gather(
            self._search_es.search_collection_movies(
                movie_id=movie.movie_id,
                collection_name=movie.collection_name,
            ),
            self._search_es.search_related_movies(
                movie_id=movie.movie_id,
                title=movie.title,
                title_en=movie.title_en,
                overview=movie.overview,
                director=movie.director,
                cast_members=movie.get_cast_list(),
                genres=movie.get_genres_list(),
                collection_name=movie.collection_name,
                limit=max(limit * 2, 30),
            ),
        )
        if not collection_movies and not es_movies:
            return []

        title_lookup = await self._build_title_lookup(
            es_movies=self._select_title_lookup_es_movies(
                collection_movies=collection_movies or [],
                candidate_movies=es_movies or [],
                limit=limit,
            ),
        )
        return self._build_es_related_movie_items(
            source_movie=movie,
            collection_movies=collection_movies or [],
            candidate_movies=es_movies or [],
            limit=limit,
            include_collection_movies=False,
            title_lookup=title_lookup,
        )

    def _merge_candidate_maps(
        self,
        merged_map: dict[str, RelatedCandidate],
        partial_map: dict[str, RelatedCandidate],
    ) -> None:
        """외부 소스별 후보 맵을 합칩니다."""
        for external_id, candidate in partial_map.items():
            target = merged_map.setdefault(external_id, RelatedCandidate())
            target.score += candidate.score
            target.qdrant_vector_similarity = max(
                target.qdrant_vector_similarity,
                candidate.qdrant_vector_similarity,
            )
            if candidate.qdrant_vector_rank is not None:
                if target.qdrant_vector_rank is None:
                    target.qdrant_vector_rank = candidate.qdrant_vector_rank
                else:
                    target.qdrant_vector_rank = min(target.qdrant_vector_rank, candidate.qdrant_vector_rank)
            for reason in candidate.reasons:
                if reason not in target.reasons:
                    target.reasons.append(reason)
            for source in candidate.sources:
                if source not in target.sources:
                    target.sources.append(source)

    async def _build_title_lookup(
        self,
        *,
        dto_movies: list[MovieDTO] | None = None,
        es_movies: list[ESSearchMovieItem] | None = None,
        limit: int = 200,
    ) -> dict[str, list[MovieDTO]]:
        """무효 포스터 후보를 제목 exact match 영화로 치환하기 위한 lookup을 구성합니다."""
        titles: list[str] = []
        for movie in dto_movies or []:
            if self._has_movie_poster(movie):
                continue
            titles.extend(collect_exact_title_candidates(movie.title, movie.title_en))
        for movie in es_movies or []:
            if self._has_es_poster(movie):
                continue
            titles.extend(collect_exact_title_candidates(movie.title, movie.title_en))

        if not titles:
            return {}

        normalized_limit = max(1, min(limit, 200))
        unique_titles = list(dict.fromkeys(titles))[:normalized_limit]
        candidates = await self._movie_repo.find_with_posters_by_titles(
            unique_titles,
            limit=min(normalized_limit * 2, 200),
        )
        lookup: dict[str, dict[str, MovieDTO]] = {}
        for candidate in candidates:
            for title in collect_exact_title_candidates(candidate.title, candidate.title_en):
                bucket = lookup.setdefault(title, {})
                bucket[candidate.movie_id] = candidate
        return {
            title: list(bucket.values())
            for title, bucket in lookup.items()
        }

    @classmethod
    def _title_lookup_window_size(cls, limit: int) -> int:
        """title fallback lookup에 사용할 후보 영화 최대 개수입니다."""
        return min(
            max(limit * cls._TITLE_LOOKUP_WINDOW_MULTIPLIER, cls._TITLE_LOOKUP_MIN_CANDIDATES),
            cls._TITLE_LOOKUP_MAX_CANDIDATES,
        )

    def _select_title_lookup_dto_movies(
        self,
        *,
        collection_movies: list[MovieDTO],
        candidate_map: dict[str, RelatedCandidate],
        candidate_movies: list[MovieDTO],
        limit: int,
    ) -> list[MovieDTO]:
        """Qdrant 후보 중 상위 노출권 영화만 골라 title lookup 비용을 줄입니다."""
        lookup_window = self._title_lookup_window_size(limit)
        selected: list[MovieDTO] = []
        seen_ids: set[str] = set()

        for movie in self._sort_collection_movie_dtos(collection_movies):
            movie_id = str(movie.movie_id or "").strip()
            if not movie_id or movie_id in seen_ids:
                continue
            selected.append(movie)
            seen_ids.add(movie_id)
            if len(selected) >= lookup_window:
                return selected

        for movie in sorted(
            candidate_movies,
            key=lambda item: self._candidate_lookup_sort_key(item, candidate_map),
            reverse=True,
        ):
            movie_id = str(movie.movie_id or "").strip()
            if not movie_id or movie_id in seen_ids:
                continue
            selected.append(movie)
            seen_ids.add(movie_id)
            if len(selected) >= lookup_window:
                break

        return selected

    def _select_title_lookup_es_movies(
        self,
        *,
        collection_movies: list[ESSearchMovieItem],
        candidate_movies: list[ESSearchMovieItem],
        limit: int,
    ) -> list[ESSearchMovieItem]:
        """ES fallback 결과에서도 상위 일부만 title lookup 대상으로 삼습니다."""
        lookup_window = self._title_lookup_window_size(limit)
        selected: list[ESSearchMovieItem] = []
        seen_ids: set[str] = set()

        for movie in [*collection_movies, *candidate_movies]:
            movie_id = str(movie.movie_id or "").strip()
            if not movie_id or movie_id in seen_ids:
                continue
            selected.append(movie)
            seen_ids.add(movie_id)
            if len(selected) >= lookup_window:
                break

        return selected

    def _candidate_lookup_sort_key(
        self,
        movie: MovieDTO,
        candidate_map: dict[str, RelatedCandidate],
    ) -> tuple[float, int, float, float, int, int]:
        """동일 영화에 연결된 후보 점수 중 가장 강한 값을 기준으로 lookup 우선순위를 정합니다."""
        best_similarity = 0.0
        best_rank = 10_000
        best_score = 0.0

        for identifier in self._build_movie_identifiers(movie):
            candidate = candidate_map.get(identifier)
            if candidate is None:
                continue
            best_similarity = max(best_similarity, candidate.qdrant_vector_similarity)
            best_score = max(best_score, candidate.score)
            if candidate.qdrant_vector_rank is not None:
                best_rank = min(best_rank, candidate.qdrant_vector_rank)

        return (
            best_similarity,
            -best_rank,
            best_score,
            movie.rating or 0.0,
            movie.vote_count or 0,
            movie.release_year or 0,
        )

    def _build_es_related_movie_items(
        self,
        *,
        source_movie: MovieDTO,
        collection_movies: list[ESSearchMovieItem],
        candidate_movies: list[ESSearchMovieItem],
        limit: int,
        include_collection_movies: bool = True,
        title_lookup: dict[str, list[MovieDTO]] | None = None,
    ) -> list[RelatedMovieItem]:
        """ES 검색 결과를 연관 영화 응답 모델로 변환합니다."""
        resolved_title_lookup = title_lookup or {}
        collection_movie_entries = [
            (movie, self._resolve_display_movie_from_es(movie, resolved_title_lookup))
            for movie in collection_movies
        ]
        collection_movie_entries = [
            (movie, display_movie)
            for movie, display_movie in collection_movie_entries
            if display_movie is not None
        ]
        candidate_movie_entries = [
            (movie, self._resolve_display_movie_from_es(movie, resolved_title_lookup))
            for movie in candidate_movies
        ]
        candidate_movie_entries = [
            (movie, display_movie)
            for movie, display_movie in candidate_movie_entries
            if display_movie is not None
        ]
        source_genres = set(source_movie.get_genres_list())
        source_cast = set(source_movie.get_cast_list())
        source_director = (source_movie.director or "").strip()
        source_collection = (source_movie.collection_name or "").strip()
        collection_ids = {
            self._resolved_movie_id(display_movie)
            for _movie, display_movie in collection_movie_entries
            if self._resolved_movie_id(display_movie)
        }
        desired_total = max(limit, len(collection_ids)) if include_collection_movies else limit
        related_movie_map = {
            movie.movie_id: (movie, display_movie)
            for movie, display_movie in candidate_movie_entries
            if movie.movie_id
        }

        related_items: list[RelatedMovieItem] = []
        seen_ids: set[str] = set()

        if include_collection_movies:
            for collection_movie, display_movie in sorted(
                collection_movie_entries,
                key=lambda item: (
                    item[0].release_year is None,
                    item[0].release_year or 0,
                    -(item[0].vote_count or 0),
                    item[0].title,
                ),
            ):
                merged_movie, merged_display_movie = related_movie_map.get(
                    collection_movie.movie_id,
                    (collection_movie, display_movie),
                )
                self._append_es_related_movie_item(
                    related_items=related_items,
                    seen_ids=seen_ids,
                    source_movie=source_movie,
                    source_genres=source_genres,
                    source_cast=source_cast,
                    source_director=source_director,
                    source_collection=source_collection,
                    candidate=merged_movie,
                    display_movie=merged_display_movie,
                    relation_sources=["elasticsearch_collection", "elasticsearch_related"]
                    if merged_movie.movie_id in related_movie_map
                    else ["elasticsearch_collection"],
                )

        sorted_candidate_entries = sorted(
            candidate_movie_entries,
            key=lambda item: (
                self._calculate_es_relation_score(
                    source_genres=source_genres,
                    source_cast=source_cast,
                    source_director=source_director,
                    source_collection=source_collection,
                    candidate=item[0],
                ),
                item[0].rating or 0.0,
                item[0].vote_count or 0,
                item[0].release_year or 0,
            ),
            reverse=True,
        )
        for movie, display_movie in sorted_candidate_entries:
            if len(related_items) >= desired_total:
                break
            candidate_collection = (movie.collection_name or "").strip()
            if (
                not include_collection_movies
                and (
                    self._resolved_movie_id(display_movie) in collection_ids
                    or (
                        source_collection
                        and candidate_collection
                        and source_collection == candidate_collection
                    )
                )
            ):
                continue
            self._append_es_related_movie_item(
                related_items=related_items,
                seen_ids=seen_ids,
                source_movie=source_movie,
                source_genres=source_genres,
                source_cast=source_cast,
                source_director=source_director,
                source_collection=source_collection,
                candidate=movie,
                display_movie=display_movie,
                relation_sources=["elasticsearch_related"],
            )

        return related_items

    def _build_collection_movie_items(
        self,
        collection_movies: list[MovieDTO],
        *,
        title_lookup: dict[str, list[MovieDTO]] | None = None,
    ) -> list[RelatedMovieItem]:
        """같은 컬렉션 작품만 별도 섹션용 응답으로 변환합니다."""
        related_items: list[RelatedMovieItem] = []
        seen_movie_ids: set[str] = set()
        for collection_movie in self._sort_collection_movie_dtos(collection_movies):
            display_movie = self._resolve_display_movie_dto(collection_movie, title_lookup or {})
            if display_movie is None or display_movie.movie_id in seen_movie_ids:
                continue
            seen_movie_ids.add(display_movie.movie_id)
            collection_candidate = RelatedCandidate()
            self._decorate_collection_candidate(
                collection_candidate,
                collection_movie.collection_name or display_movie.collection_name,
            )
            related_items.append(
                self._to_related_movie_item(movie=display_movie, candidate=collection_candidate)
            )
        return related_items

    def _build_collection_es_movie_items(
        self,
        collection_movies: list[ESSearchMovieItem],
        *,
        title_lookup: dict[str, list[MovieDTO]] | None = None,
    ) -> list[RelatedMovieItem]:
        """ES 컬렉션 검색 결과를 컬렉션 전용 응답 모델로 변환합니다."""
        related_items: list[RelatedMovieItem] = []
        seen_movie_ids: set[str] = set()
        for collection_movie in self._sort_collection_movies(collection_movies):
            display_movie = self._resolve_display_movie_from_es(collection_movie, title_lookup or {})
            resolved_movie_id = self._resolved_movie_id(display_movie)
            if display_movie is None or not resolved_movie_id or resolved_movie_id in seen_movie_ids:
                continue
            seen_movie_ids.add(resolved_movie_id)

            collection_candidate = RelatedCandidate(sources=["elasticsearch_collection"])
            self._decorate_collection_candidate(
                collection_candidate,
                collection_movie.collection_name
                or getattr(display_movie, "collection_name", None),
            )
            if isinstance(display_movie, MovieDTO):
                related_items.append(
                    self._to_related_movie_item(movie=display_movie, candidate=collection_candidate)
                )
                continue

            related_items.append(
                self._to_related_movie_item_from_es(movie=display_movie, candidate=collection_candidate)
            )
        return related_items

    def _build_es_relation_reasons(
        self,
        *,
        source_genres: set[str],
        source_cast: set[str],
        source_director: str,
        source_collection: str,
        candidate: ESSearchMovieItem,
    ) -> list[str]:
        """ES 후보와 기준 영화의 공통 메타데이터를 바탕으로 추천 사유를 구성합니다."""
        reasons: list[str] = []

        candidate_collection = (candidate.collection_name or "").strip()
        if source_collection and candidate_collection and source_collection == candidate_collection:
            reasons.append(f"같은 컬렉션: {source_collection}")

        shared_genres = [genre for genre in candidate.genres if genre in source_genres]
        if shared_genres:
            reasons.append(
                f"같은 장르: {shared_genres[0]}"
                if len(shared_genres) == 1
                else f"공통 장르 {len(shared_genres)}개"
            )

        shared_cast = [actor for actor in candidate.cast if actor in source_cast]
        if shared_cast:
            reasons.append(
                f"공통 출연: {shared_cast[0]}"
                if len(shared_cast) == 1
                else f"공통 출연 {len(shared_cast)}명"
            )

        candidate_director = (candidate.director or "").strip()
        if source_director and candidate_director and source_director == candidate_director:
            reasons.append(f"같은 감독: {source_director}")

        if not reasons:
            reasons.append("줄거리/키워드 유사")

        return reasons

    def _append_es_related_movie_item(
        self,
        *,
        related_items: list[RelatedMovieItem],
        seen_ids: set[str],
        source_movie: MovieDTO,
        source_genres: set[str],
        source_cast: set[str],
        source_director: str,
        source_collection: str,
        candidate: ESSearchMovieItem,
        display_movie: ESSearchMovieItem | MovieDTO | None,
        relation_sources: list[str],
    ) -> None:
        resolved_movie_id = self._resolved_movie_id(display_movie)
        if (
            display_movie is None
            or not resolved_movie_id
            or resolved_movie_id == source_movie.movie_id
            or resolved_movie_id in seen_ids
        ):
            return

        seen_ids.add(resolved_movie_id)
        reasons = self._build_es_relation_reasons(
            source_genres=source_genres,
            source_cast=source_cast,
            source_director=source_director,
            source_collection=source_collection,
            candidate=candidate,
        )
        relation_candidate = RelatedCandidate(
            score=round(
                self._calculate_es_relation_score(
                    source_genres=source_genres,
                    source_cast=source_cast,
                    source_director=source_director,
                    source_collection=source_collection,
                    candidate=candidate,
                ),
                4,
            ),
            reasons=reasons[:3],
            sources=relation_sources,
        )
        if isinstance(display_movie, MovieDTO):
            related_items.append(
                self._to_related_movie_item(movie=display_movie, candidate=relation_candidate)
            )
            return

        related_items.append(
            self._to_related_movie_item_from_es(movie=display_movie, candidate=relation_candidate)
        )

    def _sort_collection_movies(
        self,
        collection_movies: list[ESSearchMovieItem],
    ) -> list[ESSearchMovieItem]:
        return sorted(
            collection_movies,
            key=lambda movie: (
                movie.release_year is None,
                movie.release_year or 0,
                -(movie.vote_count or 0),
                movie.title,
            ),
        )

    def _sort_general_es_candidates(
        self,
        *,
        candidate_movies: list[ESSearchMovieItem],
        source_genres: set[str],
        source_cast: set[str],
        source_director: str,
        source_collection: str,
    ) -> list[ESSearchMovieItem]:
        return sorted(
            candidate_movies,
            key=lambda movie: (
                self._calculate_es_relation_score(
                    source_genres=source_genres,
                    source_cast=source_cast,
                    source_director=source_director,
                    source_collection=source_collection,
                    candidate=movie,
                ),
                movie.rating or 0.0,
                movie.vote_count or 0,
                movie.release_year or 0,
            ),
            reverse=True,
        )

    def _calculate_es_relation_score(
        self,
        *,
        source_genres: set[str],
        source_cast: set[str],
        source_director: str,
        source_collection: str,
        candidate: ESSearchMovieItem,
    ) -> float:
        base_score = float(candidate.score or 0.0)
        candidate_collection = (candidate.collection_name or "").strip()
        candidate_director = (candidate.director or "").strip()
        shared_genre_count = len([genre for genre in candidate.genres if genre in source_genres])
        shared_cast_count = len([actor for actor in candidate.cast if actor in source_cast])
        same_collection = bool(
            source_collection and candidate_collection and source_collection == candidate_collection
        )
        same_director = bool(
            source_director and candidate_director and source_director == candidate_director
        )

        adjusted_score = base_score * 3.0
        adjusted_score += shared_genre_count * 18.0
        adjusted_score += shared_cast_count * 7.0

        if same_collection:
            adjusted_score += 90.0
        if same_director:
            adjusted_score += 4.0
            if not same_collection and shared_genre_count == 0 and shared_cast_count == 0:
                adjusted_score -= 12.0

        return adjusted_score

    async def _fetch_qdrant_candidates(
        self,
        movie: MovieDTO,
        limit: int,
    ) -> dict[str, RelatedCandidate]:
        """
        Qdrant point payload + recommend API로 연관 후보를 수집합니다.

        1. 기준 영화 point 탐색
        2. payload 내 similar/recommendation ids 반영
        3. vector recommend 결과를 추가 반영
        """
        candidate_map: dict[str, RelatedCandidate] = {}
        timeout = httpx.Timeout(self._settings.RELATED_MOVIE_HTTP_TIMEOUT_SEC)

        async with httpx.AsyncClient(timeout=timeout) as client:
            point = await self._resolve_qdrant_point(client, movie)
            if point is None:
                return candidate_map

            point_id = point.get("id")
            payload = point.get("payload") or {}

            self._merge_identifier_list(
                candidate_map,
                payload.get("similar_movie_ids"),
                score=100.0,
                reason="비슷한 분위기의 작품",
                source="qdrant_similar_ids",
            )
            self._merge_identifier_list(
                candidate_map,
                payload.get("recommendation_ids"),
                score=100.0,
                reason="함께 추천되는 작품",
                source="qdrant_recommendation_ids",
            )

            if point_id is None:
                return candidate_map

            vector_candidates = await self._fetch_qdrant_vector_neighbors(
                client,
                point_id=point_id,
                limit=limit,
            )
            for index, item in enumerate(vector_candidates):
                candidate_id = str(item.get("id", "")).strip()
                if not candidate_id:
                    continue

                similarity = float(item.get("score") or 0.0)
                rank_bonus = max(0.0, float(limit - index))
                self._merge_candidate(
                    candidate_map,
                    external_id=candidate_id,
                    score=similarity * 10.0 + rank_bonus * 0.5,
                    reason="비슷한 줄거리",
                    source="qdrant_plot_vector",
                    qdrant_vector_similarity=similarity,
                    qdrant_vector_rank=index,
                )

        return candidate_map

    async def _resolve_qdrant_point(
        self,
        client: httpx.AsyncClient,
        movie: MovieDTO,
    ) -> dict[str, Any] | None:
        """기준 영화에 대응하는 Qdrant point를 식별자로 찾아 반환합니다."""
        if movie.tmdb_id is not None:
            response = await client.get(
                f"{self._settings.QDRANT_URL}/collections/{self._settings.QDRANT_COLLECTION}/points/{movie.tmdb_id}",
                params={"with_payload": "true", "with_vector": "false"},
            )
            if response.is_success:
                point = response.json().get("result")
                if point is not None:
                    return point

        lookup_attempts: list[tuple[str, str | None]] = [
            ("imdb_id", movie.imdb_id),
            ("kobis_movie_cd", movie.kobis_movie_cd),
        ]
        for key, value in lookup_attempts:
            point = await self._scroll_qdrant_point(
                client,
                filters=[{"key": key, "match": {"value": value}}],
            ) if value else None
            if point is not None:
                return point

        # 마지막 폴백: 제목 + 연도 (+ 감독) 조합
        must_filters: list[dict[str, Any]] = []
        if movie.title:
            must_filters.append({"key": "title", "match": {"value": movie.title}})
        if movie.release_year:
            must_filters.append({"key": "release_year", "match": {"value": movie.release_year}})
        if movie.director:
            must_filters.append({"key": "director", "match": {"value": movie.director}})
        if must_filters:
            return await self._scroll_qdrant_point(client, filters=must_filters)

        return None

    async def _scroll_qdrant_point(
        self,
        client: httpx.AsyncClient,
        filters: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Qdrant scroll 필터로 단건 point를 조회합니다."""
        response = await client.post(
            f"{self._settings.QDRANT_URL}/collections/{self._settings.QDRANT_COLLECTION}/points/scroll",
            json={
                "limit": 1,
                "with_payload": True,
                "with_vector": False,
                "filter": {"must": filters},
            },
        )
        if not response.is_success:
            return None

        points = response.json().get("result", {}).get("points", [])
        return points[0] if points else None

    async def _fetch_qdrant_vector_neighbors(
        self,
        client: httpx.AsyncClient,
        *,
        point_id: int | str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Qdrant recommend API로 기준 point의 벡터 근접 후보를 조회합니다."""
        response = await client.post(
            f"{self._settings.QDRANT_URL}/collections/{self._settings.QDRANT_COLLECTION}/points/recommend",
            json={
                "positive": [point_id],
                "limit": limit,
                "with_payload": False,
                "with_vector": False,
            },
        )
        if not response.is_success:
            return []

        return response.json().get("result", []) or []

    async def _fetch_neo4j_candidates(
        self,
        movie: MovieDTO,
        limit: int,
    ) -> dict[str, RelatedCandidate]:
        """Neo4j 관계 그래프에서 연관 후보를 읽기 전용으로 수집합니다."""
        source_ids = self._build_movie_identifiers(movie)
        if not source_ids:
            return {}

        candidate_map: dict[str, RelatedCandidate] = {}
        timeout = httpx.Timeout(self._settings.RELATED_MOVIE_HTTP_TIMEOUT_SEC)
        top_cast = movie.get_cast_list()[:3]

        async with httpx.AsyncClient(
            timeout=timeout,
            auth=(self._settings.NEO4J_USER, self._settings.NEO4J_PASSWORD),
        ) as client:
            direct_records, collection_records, director_records, cast_records = await asyncio.gather(
                self._run_neo4j_query(
                    client,
                    statement=_NEO4J_DIRECT_RELATION_QUERY,
                    parameters={"source_ids": source_ids, "limit": limit},
                ),
                self._run_neo4j_query(
                    client,
                    statement=_NEO4J_COLLECTION_QUERY,
                    parameters={"source_ids": source_ids, "limit": limit},
                ),
                self._run_neo4j_query(
                    client,
                    statement=_NEO4J_DIRECTOR_QUERY,
                    parameters={
                        "director": movie.director,
                        "source_ids": source_ids,
                        "limit": limit,
                    },
                ) if movie.director else asyncio.sleep(0, result=[]),
                self._run_neo4j_query(
                    client,
                    statement=_NEO4J_CAST_QUERY,
                    parameters={"actors": top_cast, "source_ids": source_ids, "limit": limit},
                ) if top_cast else asyncio.sleep(0, result=[]),
            )

        for record in direct_records:
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue

            relation_types = record.get("relation_types") or []
            if "SIMILAR_TO" in relation_types:
                self._merge_candidate(
                    candidate_map,
                    external_id=candidate_id,
                    score=9.0,
                    reason="비슷한 작품",
                    source="neo4j_similar_to",
                )
            if "RECOMMENDED" in relation_types:
                self._merge_candidate(
                    candidate_map,
                    external_id=candidate_id,
                    score=5.0,
                    reason="함께 언급됨",
                    source="neo4j_recommended",
                )

        for record in collection_records:
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue

            collection_name = str(record.get("collection_name") or "").strip()
            self._merge_candidate(
                candidate_map,
                external_id=candidate_id,
                score=6.0,
                reason=f"같은 컬렉션: {collection_name}" if collection_name else "같은 컬렉션",
                source="neo4j_collection",
            )

        for record in director_records:
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue

            self._merge_candidate(
                candidate_map,
                external_id=candidate_id,
                score=2.0,
                reason=f"같은 감독: {movie.director}",
                source="neo4j_director",
            )

        for record in cast_records:
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue

            matched_actors = [str(actor).strip() for actor in (record.get("matched_actors") or []) if str(actor).strip()]
            actor_count = int(record.get("actor_count") or len(matched_actors) or 0)
            if matched_actors:
                actor_label = (
                    f"공통 출연: {matched_actors[0]}"
                    if len(matched_actors) == 1
                    else f"공통 출연 {len(matched_actors)}명"
                )
            else:
                actor_label = "공통 출연진"

            self._merge_candidate(
                candidate_map,
                external_id=candidate_id,
                score=3.0 + min(actor_count, 3) * 2.0,
                reason=actor_label,
                source="neo4j_cast",
            )

        return candidate_map

    async def _run_neo4j_query(
        self,
        client: httpx.AsyncClient,
        *,
        statement: str,
        parameters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Neo4j HTTP transactional endpoint로 읽기 전용 Cypher를 실행합니다."""
        response = await client.post(
            f"{self._settings.NEO4J_HTTP_URL}/db/neo4j/tx/commit",
            json={"statements": [{"statement": statement, "parameters": parameters}]},
        )
        response.raise_for_status()

        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            first_error = errors[0]
            raise RuntimeError(first_error.get("message") or "Neo4j query failed")

        results = payload.get("results") or []
        if not results:
            return []

        columns = results[0].get("columns") or []
        rows = results[0].get("data") or []
        normalized_records: list[dict[str, Any]] = []
        for row in rows:
            values = row.get("row") or []
            normalized_records.append(dict(zip(columns, values, strict=False)))
        return normalized_records

    def _build_related_movie_items(
        self,
        *,
        source_movie: MovieDTO,
        collection_movies: list[MovieDTO],
        candidate_map: dict[str, RelatedCandidate],
        candidate_movies: list[MovieDTO],
        limit: int,
        title_lookup: dict[str, list[MovieDTO]] | None = None,
    ) -> list[RelatedMovieItem]:
        """컬렉션을 제외한 Qdrant 벡터 우선 기준의 연관 영화 응답을 구성합니다."""
        resolved_title_lookup = title_lookup or {}
        external_id_to_movie: dict[str, MovieDTO] = {}
        for movie in candidate_movies:
            display_movie = self._resolve_display_movie_dto(movie, resolved_title_lookup)
            if display_movie is None:
                continue
            for identifier in self._build_movie_identifiers(movie):
                external_id_to_movie[identifier] = display_movie

        by_canonical_id: dict[str, tuple[MovieDTO, RelatedCandidate]] = {}
        source_identifiers = set(self._build_movie_identifiers(source_movie))
        source_collection = (source_movie.collection_name or "").strip()

        for external_id, candidate in candidate_map.items():
            movie = external_id_to_movie.get(external_id)
            if movie is None:
                continue
            if movie.movie_id == source_movie.movie_id:
                continue
            if external_id in source_identifiers:
                continue
            candidate_collection = (movie.collection_name or "").strip()
            if source_collection and candidate_collection and source_collection == candidate_collection:
                continue

            existing = by_canonical_id.get(movie.movie_id)
            if existing is None:
                by_canonical_id[movie.movie_id] = (
                    movie,
                    RelatedCandidate(
                        score=candidate.score,
                        qdrant_vector_similarity=candidate.qdrant_vector_similarity,
                        qdrant_vector_rank=candidate.qdrant_vector_rank,
                        reasons=list(candidate.reasons),
                        sources=list(candidate.sources),
                    ),
                )
                continue

            merged_movie, merged_candidate = existing
            merged_candidate.score += candidate.score
            merged_candidate.qdrant_vector_similarity = max(
                merged_candidate.qdrant_vector_similarity,
                candidate.qdrant_vector_similarity,
            )
            if candidate.qdrant_vector_rank is not None:
                if merged_candidate.qdrant_vector_rank is None:
                    merged_candidate.qdrant_vector_rank = candidate.qdrant_vector_rank
                else:
                    merged_candidate.qdrant_vector_rank = min(
                        merged_candidate.qdrant_vector_rank,
                        candidate.qdrant_vector_rank,
                    )
            for reason in candidate.reasons:
                if reason not in merged_candidate.reasons:
                    merged_candidate.reasons.append(reason)
            for source in candidate.sources:
                if source not in merged_candidate.sources:
                    merged_candidate.sources.append(source)
            by_canonical_id[movie.movie_id] = (merged_movie, merged_candidate)

        collection_movie_ids = {
            display_movie.movie_id
            for display_movie in (
                self._resolve_display_movie_dto(movie, resolved_title_lookup)
                for movie in collection_movies
            )
            if display_movie is not None
        }
        desired_total = limit
        related_items: list[RelatedMovieItem] = []
        seen_ids: set[str] = set(collection_movie_ids)

        sorted_movies = sorted(
            by_canonical_id.values(),
            key=lambda item: (
                item[1].qdrant_vector_similarity,
                -(item[1].qdrant_vector_rank if item[1].qdrant_vector_rank is not None else 10_000),
                item[1].score,
                item[0].rating or 0.0,
                item[0].vote_count or 0,
                item[0].release_year or 0,
            ),
            reverse=True,
        )

        for movie, candidate in sorted_movies:
            if len(related_items) >= desired_total:
                break
            if movie.movie_id in seen_ids:
                continue
            related_items.append(self._to_related_movie_item(movie=movie, candidate=candidate))
            seen_ids.add(movie.movie_id)

        return related_items

    def _has_movie_poster(self, movie: MovieDTO) -> bool:
        """MySQL 영화 DTO가 유효한 포스터 경로를 갖는지 확인합니다."""
        return is_valid_internal_poster_path(movie.poster_path)

    def _has_es_poster(self, movie: ESSearchMovieItem) -> bool:
        """ES 검색 결과가 유효한 포스터 경로를 갖는지 확인합니다."""
        return is_valid_internal_poster_path(movie.poster_path)

    def _to_related_movie_item(
        self,
        *,
        movie: MovieDTO,
        candidate: RelatedCandidate,
    ) -> RelatedMovieItem:
        """MovieDTO를 연관 영화 응답 모델로 변환합니다."""
        poster_url = build_tmdb_poster_url(self._settings.TMDB_IMAGE_BASE_URL, movie.poster_path)

        return RelatedMovieItem(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=movie.get_genres_list(),
            release_year=movie.release_year,
            rating=movie.rating,
            vote_count=movie.vote_count,
            poster_url=poster_url,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
            relation_score=round(candidate.score, 4),
            relation_reasons=self._prioritize_relation_reasons(candidate.reasons)[:3],
            relation_sources=candidate.sources,
        )

    def _to_related_movie_item_from_es(
        self,
        *,
        movie: ESSearchMovieItem,
        candidate: RelatedCandidate,
    ) -> RelatedMovieItem:
        """ES 검색 결과를 연관 영화 응답 모델로 변환합니다."""
        poster_url = build_tmdb_poster_url(self._settings.TMDB_IMAGE_BASE_URL, movie.poster_path)
        return RelatedMovieItem(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=movie.genres,
            release_year=movie.release_year,
            rating=movie.rating,
            vote_count=movie.vote_count,
            poster_url=poster_url,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
            relation_score=round(candidate.score, 4),
            relation_reasons=self._prioritize_relation_reasons(candidate.reasons)[:3],
            relation_sources=candidate.sources,
        )

    def _resolve_display_movie_dto(
        self,
        movie: MovieDTO,
        title_lookup: dict[str, list[MovieDTO]],
    ) -> MovieDTO | None:
        """DB 영화의 포스터가 무효면 제목 exact match 후보로 대체합니다."""
        if self._has_movie_poster(movie):
            return movie
        return self._select_title_fallback_movie(
            title=movie.title,
            title_en=movie.title_en,
            title_lookup=title_lookup,
            exclude_movie_id=movie.movie_id,
        )

    def _resolve_display_movie_from_es(
        self,
        movie: ESSearchMovieItem,
        title_lookup: dict[str, list[MovieDTO]],
    ) -> ESSearchMovieItem | MovieDTO | None:
        """ES 영화의 포스터가 무효면 제목 exact match 후보로 대체합니다."""
        if self._has_es_poster(movie):
            return movie
        return self._select_title_fallback_movie(
            title=movie.title,
            title_en=movie.title_en,
            title_lookup=title_lookup,
        )

    @staticmethod
    def _select_title_fallback_movie(
        *,
        title: str | None,
        title_en: str | None,
        title_lookup: dict[str, list[MovieDTO]],
        exclude_movie_id: str | None = None,
    ) -> MovieDTO | None:
        """제목 exact match 후보 중 첫 번째 유효 포스터 영화를 선택합니다."""
        for candidate_title in collect_exact_title_candidates(title, title_en):
            for candidate in title_lookup.get(candidate_title, []):
                if exclude_movie_id and candidate.movie_id == exclude_movie_id:
                    continue
                if is_valid_internal_poster_path(candidate.poster_path):
                    return candidate
        return None

    @staticmethod
    def _resolved_movie_id(movie: ESSearchMovieItem | MovieDTO | None) -> str | None:
        """대체 후 표시할 영화의 ID를 반환합니다."""
        movie_id = str(getattr(movie, "movie_id", "") or "").strip()
        return movie_id or None

    def _sort_collection_movie_dtos(
        self,
        collection_movies: list[MovieDTO],
    ) -> list[MovieDTO]:
        return sorted(
            collection_movies,
            key=lambda movie: (
                movie.release_year is None,
                movie.release_year or 0,
                -(movie.vote_count or 0),
                movie.title,
            ),
        )

    def _decorate_collection_candidate(
        self,
        candidate: RelatedCandidate,
        collection_name: str | None,
    ) -> None:
        normalized_collection_name = (collection_name or "").strip()
        collection_reason = (
            f"같은 컬렉션: {normalized_collection_name}"
            if normalized_collection_name
            else "같은 컬렉션"
        )
        if collection_reason not in candidate.reasons:
            candidate.reasons.insert(0, collection_reason)
        if "collection_priority" not in candidate.sources:
            candidate.sources.insert(0, "collection_priority")
        candidate.score += 40.0

    def _prioritize_relation_reasons(
        self,
        reasons: list[str],
    ) -> list[str]:
        def reason_priority(reason: str) -> tuple[int, str]:
            if reason.startswith("같은 컬렉션"):
                return (0, reason)
            if reason.startswith("비슷한 줄거리"):
                return (1, reason)
            if reason.startswith("비슷한 분위기의 작품"):
                return (2, reason)
            if reason.startswith("함께 추천되는 작품"):
                return (3, reason)
            if reason.startswith("비슷한 작품"):
                return (4, reason)
            if reason.startswith("함께 언급됨"):
                return (5, reason)
            if "장르" in reason:
                return (6, reason)
            if reason.startswith("공통 출연"):
                return (7, reason)
            if reason.startswith("같은 감독"):
                return (8, reason)
            return (9, reason)

        ordered = sorted(
            [reason for reason in reasons if reason],
            key=reason_priority,
        )
        return list(dict.fromkeys(ordered))

    def _build_movie_identifiers(self, movie: MovieDTO) -> list[str]:
        """MySQL/Qdrant/Neo4j 사이에 조인할 수 있는 영화 식별자 목록을 구성합니다."""
        identifiers = [movie.movie_id]
        if movie.tmdb_id is not None:
            identifiers.append(str(movie.tmdb_id))
        if movie.imdb_id:
            identifiers.append(movie.imdb_id)
        if movie.kobis_movie_cd:
            identifiers.append(movie.kobis_movie_cd)
        return list(dict.fromkeys([identifier.strip() for identifier in identifiers if identifier and identifier.strip()]))

    def _merge_identifier_list(
        self,
        candidate_map: dict[str, RelatedCandidate],
        identifiers: Any,
        *,
        score: float,
        reason: str,
        source: str,
    ) -> None:
        """ID 목록을 동일한 사유/가중치로 후보 맵에 반영합니다."""
        if not isinstance(identifiers, list):
            return

        for identifier in identifiers:
            external_id = str(identifier or "").strip()
            if not external_id:
                continue
            self._merge_candidate(
                candidate_map,
                external_id=external_id,
                score=score,
                reason=reason,
                source=source,
            )

    def _merge_candidate(
        self,
        candidate_map: dict[str, RelatedCandidate],
        *,
        external_id: str,
        score: float,
        reason: str | None,
        source: str,
        qdrant_vector_similarity: float | None = None,
        qdrant_vector_rank: int | None = None,
    ) -> None:
        """단일 후보를 맵에 누적합니다."""
        candidate = candidate_map.setdefault(external_id, RelatedCandidate())
        candidate.merge(
            score=score,
            reason=reason,
            source=source,
            qdrant_vector_similarity=qdrant_vector_similarity,
            qdrant_vector_rank=qdrant_vector_rank,
        )
