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
    _CACHE_VERSION = "v4"

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
            related_items = self._build_collection_es_movie_items(collection_movies_from_es)
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
        related_items = self._build_collection_movie_items(collection_movies)
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
                limit=max(normalized_limit * 4, 80) if movie.collection_name else max(normalized_limit * 2, 20),
            )
            qdrant_fetch_succeeded = True
            self._merge_candidate_maps(candidate_map, qdrant_result)
        except Exception as exc:
            logger.warning("related_movies_qdrant_failed movie_id=%s error=%s", movie.movie_id, exc)

        candidate_movies = await self._movie_repo.find_by_identifiers(list(candidate_map.keys())) if candidate_map else []
        related_items = self._build_related_movie_items(
            source_movie=movie,
            collection_movies=collection_movies,
            candidate_map=candidate_map,
            candidate_movies=candidate_movies,
            limit=normalized_limit,
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
                limit=max(limit * 2, 50),
            ),
        )
        if not collection_movies and not es_movies:
            return []

        return self._build_es_related_movie_items(
            source_movie=movie,
            collection_movies=collection_movies or [],
            candidate_movies=es_movies or [],
            limit=limit,
            include_collection_movies=False,
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

    def _build_es_related_movie_items(
        self,
        *,
        source_movie: MovieDTO,
        collection_movies: list[ESSearchMovieItem],
        candidate_movies: list[ESSearchMovieItem],
        limit: int,
        include_collection_movies: bool = True,
    ) -> list[RelatedMovieItem]:
        """ES 검색 결과를 연관 영화 응답 모델로 변환합니다."""
        collection_movies_with_poster = [
            movie for movie in collection_movies if self._has_es_poster(movie)
        ]
        candidate_movies_with_poster = [
            movie for movie in candidate_movies if self._has_es_poster(movie)
        ]
        source_genres = set(source_movie.get_genres_list())
        source_cast = set(source_movie.get_cast_list())
        source_director = (source_movie.director or "").strip()
        source_collection = (source_movie.collection_name or "").strip()
        collection_ids = {
            movie.movie_id
            for movie in collection_movies_with_poster
            if movie.movie_id
        }
        desired_total = max(limit, len(collection_ids)) if include_collection_movies else limit
        related_movie_map = {
            movie.movie_id: movie
            for movie in candidate_movies_with_poster
            if movie.movie_id
        }

        related_items: list[RelatedMovieItem] = []
        seen_ids: set[str] = set()

        if include_collection_movies:
            for collection_movie in self._sort_collection_movies(collection_movies_with_poster):
                merged_movie = related_movie_map.get(collection_movie.movie_id, collection_movie)
                self._append_es_related_movie_item(
                    related_items=related_items,
                    seen_ids=seen_ids,
                    source_movie=source_movie,
                    source_genres=source_genres,
                    source_cast=source_cast,
                    source_director=source_director,
                    source_collection=source_collection,
                    candidate=merged_movie,
                    relation_sources=["elasticsearch_collection", "elasticsearch_related"]
                    if merged_movie.movie_id in related_movie_map
                    else ["elasticsearch_collection"],
                )

        for movie in self._sort_general_es_candidates(
            candidate_movies=candidate_movies_with_poster,
            source_genres=source_genres,
            source_cast=source_cast,
            source_director=source_director,
            source_collection=source_collection,
        ):
            if len(related_items) >= desired_total:
                break
            candidate_collection = (movie.collection_name or "").strip()
            if (
                not include_collection_movies
                and (
                    movie.movie_id in collection_ids
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
                relation_sources=["elasticsearch_related"],
            )

        return related_items

    def _build_collection_movie_items(
        self,
        collection_movies: list[MovieDTO],
    ) -> list[RelatedMovieItem]:
        """같은 컬렉션 작품만 별도 섹션용 응답으로 변환합니다."""
        related_items: list[RelatedMovieItem] = []
        for collection_movie in self._sort_collection_movie_dtos(collection_movies):
            if not self._has_movie_poster(collection_movie):
                continue
            collection_candidate = RelatedCandidate()
            self._decorate_collection_candidate(collection_candidate, collection_movie.collection_name)
            related_items.append(
                self._to_related_movie_item(movie=collection_movie, candidate=collection_candidate)
            )
        return related_items

    def _build_collection_es_movie_items(
        self,
        collection_movies: list[ESSearchMovieItem],
    ) -> list[RelatedMovieItem]:
        """ES 컬렉션 검색 결과를 컬렉션 전용 응답 모델로 변환합니다."""
        related_items: list[RelatedMovieItem] = []
        for collection_movie in self._sort_collection_movies(collection_movies):
            if not collection_movie.movie_id or not self._has_es_poster(collection_movie):
                continue

            collection_candidate = RelatedCandidate(sources=["elasticsearch_collection"])
            self._decorate_collection_candidate(collection_candidate, collection_movie.collection_name)

            poster_url = None
            if collection_movie.poster_path:
                poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{collection_movie.poster_path}"

            related_items.append(
                RelatedMovieItem(
                    movie_id=collection_movie.movie_id,
                    title=collection_movie.title,
                    title_en=collection_movie.title_en,
                    genres=collection_movie.genres,
                    release_year=collection_movie.release_year,
                    rating=collection_movie.rating,
                    vote_count=collection_movie.vote_count,
                    poster_url=poster_url,
                    trailer_url=collection_movie.trailer_url,
                    overview=collection_movie.overview,
                    relation_score=round(collection_candidate.score, 4),
                    relation_reasons=self._prioritize_relation_reasons(collection_candidate.reasons)[:3],
                    relation_sources=collection_candidate.sources,
                )
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
        relation_sources: list[str],
    ) -> None:
        if (
            not candidate.movie_id
            or candidate.movie_id == source_movie.movie_id
            or candidate.movie_id in seen_ids
            or not self._has_es_poster(candidate)
        ):
            return

        seen_ids.add(candidate.movie_id)
        reasons = self._build_es_relation_reasons(
            source_genres=source_genres,
            source_cast=source_cast,
            source_director=source_director,
            source_collection=source_collection,
            candidate=candidate,
        )

        poster_url = None
        if candidate.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{candidate.poster_path}"

        related_items.append(
            RelatedMovieItem(
                movie_id=candidate.movie_id,
                title=candidate.title,
                title_en=candidate.title_en,
                genres=candidate.genres,
                release_year=candidate.release_year,
                rating=candidate.rating,
                vote_count=candidate.vote_count,
                poster_url=poster_url,
                trailer_url=candidate.trailer_url,
                overview=candidate.overview,
                relation_score=round(
                    self._calculate_es_relation_score(
                        source_genres=source_genres,
                        source_cast=source_cast,
                        source_director=source_director,
                        source_collection=source_collection,
                        candidate=candidate,
                    ),
                    4,
                ),
                relation_reasons=reasons[:3],
                relation_sources=relation_sources,
            )
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
    ) -> list[RelatedMovieItem]:
        """컬렉션을 제외한 Qdrant 벡터 우선 기준의 연관 영화 응답을 구성합니다."""
        external_id_to_movie: dict[str, MovieDTO] = {}
        for movie in candidate_movies:
            if not self._has_movie_poster(movie):
                continue
            for identifier in self._build_movie_identifiers(movie):
                external_id_to_movie[identifier] = movie

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

        collection_movies_with_poster = [
            movie for movie in collection_movies if self._has_movie_poster(movie)
        ]
        collection_movie_ids = {movie.movie_id for movie in collection_movies_with_poster}
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
            if not self._has_movie_poster(movie):
                continue
            related_items.append(self._to_related_movie_item(movie=movie, candidate=candidate))
            seen_ids.add(movie.movie_id)

        return related_items

    def _has_movie_poster(self, movie: MovieDTO) -> bool:
        """MySQL 영화 DTO가 유효한 포스터 경로를 갖는지 확인합니다."""
        return bool((movie.poster_path or "").strip())

    def _has_es_poster(self, movie: ESSearchMovieItem) -> bool:
        """ES 검색 결과가 유효한 포스터 경로를 갖는지 확인합니다."""
        return bool((movie.poster_path or "").strip())

    def _to_related_movie_item(
        self,
        *,
        movie: MovieDTO,
        candidate: RelatedCandidate,
    ) -> RelatedMovieItem:
        """MovieDTO를 연관 영화 응답 모델로 변환합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

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
