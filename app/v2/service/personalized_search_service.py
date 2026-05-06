"""
검색 페이지 초기 화면 개인화 TOP picks 서비스 (v2 Raw SQL).

목표:
- monglepick-agent 전체를 붙이지 않고 recommend 안에서 끝낸다.
- fav/review/wishlist 기반의 명시 신호와 co-watched CF, 박스오피스 fallback을 섞는다.
- SearchPage 상단 `예상 픽 TOP 10`만 우선 교체할 수 있는 가벼운 엔드포인트를 제공한다.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from itertools import combinations
import json
import logging
import math
import time

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    MovieBrief,
    PersonalizedMoviePick,
    PersonalizedMovieSection,
    PersonalizedTopPicksResponse,
)
from app.search_elasticsearch import ESSearchMovieItem, ElasticsearchSearchClient
from app.v2.model.dto import MovieDTO
from app.v2.repository.favorite_genre_repository import FavoriteGenreRepository
from app.v2.repository.favorite_movie_repository import FavoriteMovieRepository
from app.v2.repository.movie_repository import MovieRepository
from app.v2.repository.personalized_search_repository import PersonalizedSearchRepository
from app.v2.repository.review_repository import ReviewRepository
from app.v2.repository.user_preference_repository import UserPreferenceRepository
from app.v2.repository.wishlist_repository import WishlistRepository
from app.v2.service.match_cowatch_service import MatchCowatchService
from app.v2.service.poster_policy import (
    build_tmdb_poster_url,
    collect_exact_title_candidates,
    is_allowed_poster_url,
)
from app.v2.service.related_movie_service import RelatedMovieService
from app.v2.service.search_service import SearchService

logger = logging.getLogger(__name__)


@dataclass
class PersonalizedMovieRecord:
    """랭킹에 필요한 최소 영화 메타 정보."""

    movie_id: str
    title: str
    title_en: str | None = None
    genres: list[str] = field(default_factory=list)
    release_year: int | None = None
    rating: float | None = None
    vote_count: int | None = None
    poster_url: str | None = None
    trailer_url: str | None = None
    overview: str | None = None
    director: str | None = None
    cast: list[str] = field(default_factory=list)
    collection_name: str | None = None


@dataclass
class PersonalizedCandidate:
    """개인화 후보 누적 점수 구조."""

    movie: PersonalizedMovieRecord
    source_score: float = 0.0
    final_score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    preferred_genres: set[str] = field(default_factory=set)
    behavior_genres: set[str] = field(default_factory=set)

    def add(
        self,
        *,
        score: float,
        source: str,
        reason: str | None,
        preferred_genres: set[str] | None = None,
        behavior_genres: set[str] | None = None,
    ) -> None:
        self.source_score += score
        if source not in self.sources:
            self.sources.append(source)
        if reason and reason not in self.reasons:
            self.reasons.append(reason)
        if preferred_genres:
            self.preferred_genres.update(preferred_genres)
        if behavior_genres:
            self.behavior_genres.update(behavior_genres)


class PersonalizedSearchService:
    """검색 초기 화면 개인화 TOP picks 계산 서비스."""

    CACHE_PREFIX = "search:personalized_top_picks"
    CACHE_VERSION = "v6"
    CACHE_TTL_SECONDS = 1800

    DEFAULT_LIMIT = 10
    MAX_LIMIT = 20
    GENRE_SECTION_COUNT = 5
    GENRE_SECTION_LIMIT = 20
    WISHLIST_SECTION_LIMIT = 20
    SIMILAR_TASTE_LIMIT = 40
    REVIEW_SECTION_COUNT = 5
    REVIEW_SECTION_LIMIT = 25

    FAVORITE_SEED_LIMIT = 3
    WORLDCUP_SEED_LIMIT = 2
    REVIEW_SEED_LIMIT = 3
    IMPLICIT_SEED_LIMIT = 2
    WISHLIST_SEED_LIMIT = 2

    FAVORITE_MOVIE_SIGNAL_LIMIT = 6
    REVIEW_SIGNAL_LIMIT = 24
    WISHLIST_SIGNAL_LIMIT = 12
    IMPLICIT_SIGNAL_LIMIT = 4
    WATCH_SIGNAL_LIMIT = 80

    PREFERRED_GENRE_LIMIT = 4
    GENRE_CANDIDATE_LIMIT = 16
    COLLECTION_CANDIDATE_LIMIT = 8
    DIRECTOR_CANDIDATE_LIMIT = 10
    ACTOR_CANDIDATE_LIMIT = 8
    CO_WATCH_PAIR_LIMIT = 3
    CO_WATCH_CANDIDATE_LIMIT = 10
    BOX_OFFICE_FALLBACK_LIMIT = 30
    POSTER_LOOKUP_WINDOW_MULTIPLIER = 4
    POSTER_LOOKUP_MIN_CANDIDATES = 40
    POSTER_LOOKUP_MAX_CANDIDATES = 80
    POSTER_LOOKUP_MAX_TITLES = 120

    REVIEW_POSITIVE_THRESHOLD = 4.0
    CO_WATCH_RATING_THRESHOLD = 4.0

    POPULAR_MIN_RATING = 6.0
    POPULAR_MIN_VOTE_COUNT = 150

    def __init__(
        self,
        conn: aiomysql.Connection,
        redis_client: aioredis.Redis | None = None,
    ):
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(conn)
        self._favorite_genre_repo = FavoriteGenreRepository(conn)
        self._favorite_movie_repo = FavoriteMovieRepository(conn)
        self._user_preference_repo = UserPreferenceRepository(conn)
        self._wishlist_repo = WishlistRepository(conn)
        self._review_repo = ReviewRepository(conn)
        self._personalized_repo = PersonalizedSearchRepository(conn)
        self._match_cowatch_service = MatchCowatchService(conn, redis_client)
        self._search_service = SearchService(conn, redis_client)
        self._search_es = ElasticsearchSearchClient()

    async def get_top_picks(
        self,
        *,
        user_id: str,
        limit: int = DEFAULT_LIMIT,
    ) -> PersonalizedTopPicksResponse:
        """캐시 우선으로 사용자 개인화 TOP picks를 반환합니다."""
        started_at = time.perf_counter()
        normalized_limit = max(1, min(limit, self.MAX_LIMIT))
        cache_key = self._cache_key(user_id=user_id, limit=normalized_limit)

        cached_response = await self._read_cached_top_picks(cache_key)
        if cached_response is not None:
            logger.info(
                "personalized_top_picks_cache_hit user_id=%s limit=%s elapsed_ms=%.1f",
                user_id,
                normalized_limit,
                (time.perf_counter() - started_at) * 1000,
            )
            return cached_response

        response = await self._build_top_picks_response(
            user_id=user_id,
            limit=normalized_limit,
        )
        await self._write_cached_top_picks(cache_key, response)
        logger.info(
            "personalized_top_picks_built user_id=%s limit=%s candidates=%s selected=%s elapsed_ms=%.1f",
            user_id,
            normalized_limit,
            response.total_candidates,
            len(response.movies),
            (time.perf_counter() - started_at) * 1000,
        )
        return response

    async def get_cached_top_picks(
        self,
        *,
        user_id: str,
        limit: int = DEFAULT_LIMIT,
    ) -> PersonalizedTopPicksResponse | None:
        """캐시에 저장된 개인화 TOP picks만 반환합니다."""
        normalized_limit = max(1, min(limit, self.MAX_LIMIT))
        cache_key = self._cache_key(user_id=user_id, limit=normalized_limit)
        return await self._read_cached_top_picks(cache_key)

    async def refresh_top_picks(
        self,
        *,
        user_id: str,
        limit: int = DEFAULT_LIMIT,
    ) -> PersonalizedTopPicksResponse:
        """캐시를 무시하고 개인화 TOP picks를 다시 계산해 저장합니다."""
        normalized_limit = max(1, min(limit, self.MAX_LIMIT))
        cache_key = self._cache_key(user_id=user_id, limit=normalized_limit)
        response = await self._build_top_picks_response(
            user_id=user_id,
            limit=normalized_limit,
        )
        await self._write_cached_top_picks(cache_key, response)
        return response

    async def _build_top_picks_response(
        self,
        *,
        user_id: str,
        limit: int,
    ) -> PersonalizedTopPicksResponse:
        """실제 개인화 TOP picks를 계산합니다."""
        normalized_limit = max(1, min(limit, self.MAX_LIMIT))

        favorite_genres = await self._favorite_genre_repo.list_selected_by_user(user_id)
        favorite_movies = await self._favorite_movie_repo.list_by_user(user_id)
        user_preference = await self._user_preference_repo.get_by_user_id(user_id)
        worldcup_result = await self._user_preference_repo.get_worldcup_result(user_id)
        wishlist_rows = await self._wishlist_repo.list_by_user(
            user_id=user_id,
            offset=0,
            limit=self.WISHLIST_SIGNAL_LIMIT,
        )
        review_rows = await self._review_repo.list_by_user(
            user_id=user_id,
            offset=0,
            limit=self.REVIEW_SIGNAL_LIMIT,
        )
        implicit_movie_ids = await self._personalized_repo.list_top_implicit_movie_ids(
            user_id,
            limit=self.IMPLICIT_SIGNAL_LIMIT,
        )
        watched_movie_ids = await self._personalized_repo.list_watched_movie_ids(
            user_id,
            limit=self.WATCH_SIGNAL_LIMIT,
        )
        dismissed_movie_ids = await self._personalized_repo.list_dismissed_movie_ids(user_id)
        behavior_profile = await self._personalized_repo.get_behavior_profile(user_id)

        favorite_movie_ids = self._unique_ordered(
            [
                str(item.get("movie_id")).strip()
                for item in favorite_movies[: self.FAVORITE_MOVIE_SIGNAL_LIMIT]
                if item.get("movie_id")
            ]
        )
        wishlist_movie_ids = self._unique_ordered(
            [
                str(item.get("movie_id")).strip()
                for item in wishlist_rows
                if item.get("movie_id")
            ]
        )
        review_seed_rows = self._select_positive_review_rows(review_rows)
        review_movie_ids = self._unique_ordered(
            [
                str(item.get("movie_id")).strip()
                for item in review_seed_rows
                if item.get("movie_id")
            ]
        )
        worldcup_movie_ids = self._unique_ordered(
            [
                str(getattr(worldcup_result, "winner_movie_id", "") or "").strip(),
                str(getattr(worldcup_result, "runner_up_movie_id", "") or "").strip(),
            ]
        )
        user_preference_genres = user_preference.get_genres_list() if user_preference else []
        worldcup_preference_genres = self._extract_worldcup_genres(worldcup_result)

        seed_ids = self._unique_ordered(
            favorite_movie_ids
            + worldcup_movie_ids
            + review_movie_ids
            + implicit_movie_ids
            + wishlist_movie_ids
        )
        seed_movies = await self._movie_repo.find_by_ids(seed_ids) if seed_ids else []
        seed_records_by_id = self._build_records_by_id_from_dto(seed_movies)

        favorite_seed_records = self._pick_seed_records(
            favorite_movie_ids,
            seed_records_by_id,
            limit=self.FAVORITE_SEED_LIMIT,
        )
        worldcup_seed_records = self._pick_seed_records(
            worldcup_movie_ids,
            seed_records_by_id,
            limit=self.WORLDCUP_SEED_LIMIT,
        )
        review_seed_records = self._pick_seed_records(
            review_movie_ids,
            seed_records_by_id,
            limit=self.REVIEW_SEED_LIMIT,
        )
        implicit_seed_records = self._pick_seed_records(
            implicit_movie_ids,
            seed_records_by_id,
            limit=self.IMPLICIT_SEED_LIMIT,
        )
        wishlist_seed_records = self._pick_seed_records(
            wishlist_movie_ids,
            seed_records_by_id,
            limit=self.WISHLIST_SEED_LIMIT,
        )

        preferred_genres = self._build_preferred_genres(
            favorite_genres=favorite_genres,
            user_preference_genres=user_preference_genres,
            worldcup_genres=worldcup_preference_genres,
            behavior_profile=behavior_profile,
            seed_records=[
                *favorite_seed_records,
                *worldcup_seed_records,
                *review_seed_records,
                *implicit_seed_records,
                *wishlist_seed_records,
            ],
        )
        behavior_affinity = self._normalize_behavior_affinity(
            behavior_profile.get("genre_affinity")
        )
        cbf_weight, cf_weight = self._resolve_signal_weights(behavior_profile)

        exclude_ids = set(
            self._unique_ordered(
                favorite_movie_ids
                + worldcup_movie_ids
                + wishlist_movie_ids
                + review_movie_ids
                + watched_movie_ids
                + dismissed_movie_ids
            )
        )
        candidate_map: dict[str, PersonalizedCandidate] = {}

        await self._add_genre_candidates(
            candidate_map=candidate_map,
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            cbf_weight=cbf_weight,
        )

        await self._add_seed_candidates(
            candidate_map=candidate_map,
            seed_records=favorite_seed_records,
            seed_type="favorite",
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            weight=cbf_weight,
        )
        await self._add_seed_candidates(
            candidate_map=candidate_map,
            seed_records=worldcup_seed_records,
            seed_type="worldcup",
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            weight=cbf_weight,
        )
        await self._add_seed_candidates(
            candidate_map=candidate_map,
            seed_records=review_seed_records,
            seed_type="review",
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            weight=cbf_weight,
        )
        await self._add_seed_candidates(
            candidate_map=candidate_map,
            seed_records=implicit_seed_records,
            seed_type="implicit",
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            weight=cbf_weight,
        )
        await self._add_seed_candidates(
            candidate_map=candidate_map,
            seed_records=wishlist_seed_records,
            seed_type="wishlist",
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            weight=cbf_weight,
        )

        await self._add_cowatched_candidates(
            candidate_map=candidate_map,
            seed_records=[
                *favorite_seed_records,
                *worldcup_seed_records,
                *review_seed_records,
                *implicit_seed_records,
            ],
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            cf_weight=cf_weight,
        )

        await self._add_box_office_fallback(
            candidate_map=candidate_map,
            preferred_genres=preferred_genres,
            behavior_affinity=behavior_affinity,
            exclude_ids=exclude_ids,
            limit=max(self.BOX_OFFICE_FALLBACK_LIMIT, normalized_limit * 3),
        )

        ranked_candidates = self._finalize_candidates(
            candidate_map=candidate_map,
            behavior_affinity=behavior_affinity,
        )
        ranked_candidates = await self._resolve_ranked_candidate_posters(
            ranked_candidates=ranked_candidates,
            limit=normalized_limit,
        )
        selected_candidates = self._select_ranked_candidates(
            ranked_candidates=ranked_candidates,
            limit=normalized_limit,
        )
        genre_sections, wishlist_movies, similar_taste_movies, review_sections = await asyncio.gather(
            self._build_genre_sections(
                preferred_genres=preferred_genres,
            ),
            self._build_wishlist_section_movies(
                wishlist_seed_records=wishlist_seed_records,
            ),
            self._build_similar_taste_movies(
                favorite_seed_records=favorite_seed_records,
                review_rows=review_rows,
            ),
            self._build_review_sections(
                review_rows=review_rows,
                records_by_id=seed_records_by_id,
            ),
        )

        response = PersonalizedTopPicksResponse(
            movies=[self._to_pick(candidate) for candidate in selected_candidates],
            total_candidates=len(ranked_candidates),
            genre_sections=genre_sections,
            wishlist_movies=wishlist_movies,
            similar_taste_movies=similar_taste_movies,
            review_sections=review_sections,
        )
        return response

    async def _build_genre_sections(
        self,
        *,
        preferred_genres: list[str],
    ) -> list[PersonalizedMovieSection]:
        """선호 장르별 추천 섹션을 구성합니다."""
        target_genres = preferred_genres[: self.GENRE_SECTION_COUNT]
        if not target_genres:
            return []

        sections: list[PersonalizedMovieSection] = []
        if self._search_es.is_available():
            results = await asyncio.gather(
                *[
                    self._search_es.search_movies(
                        keyword=None,
                        search_type="title",
                        genre=None,
                        genres=[genre],
                        genre_match_groups=None,
                        year_from=None,
                        year_to=None,
                        rating_min=None,
                        rating_max=None,
                        popularity_min=None,
                        popularity_max=None,
                        vote_count_min=None,
                        sort_by="rating",
                        sort_order="desc",
                        page=1,
                        size=self.GENRE_SECTION_LIMIT,
                    )
                    for genre in target_genres
                ],
                return_exceptions=True,
            )
            for index, genre in enumerate(target_genres):
                result = results[index]
                if isinstance(result, Exception) or result is None:
                    logger.warning(
                        "personalized_genre_section_es_failed genre=%s error=%s",
                        genre,
                        result,
                    )
                    section = await self._build_genre_section_from_db(genre)
                else:
                    section = await self._build_genre_section_from_es(
                        genre=genre,
                        movies=result.movies,
                    )
                if section is not None and section.movies:
                    sections.append(section)
            return sections

        for genre in target_genres:
            section = await self._build_genre_section_from_db(genre)
            if section is not None and section.movies:
                sections.append(section)
        return sections

    async def _build_genre_section_from_db(
        self,
        genre: str,
    ) -> PersonalizedMovieSection | None:
        """MySQL 검색 결과로 장르 섹션 하나를 구성합니다."""
        movies, _ = await self._movie_repo.search(
            genres=[genre],
            sort_by="rating",
            sort_order="desc",
            page=1,
            size=self.GENRE_SECTION_LIMIT,
        )
        picks = await self._records_to_preview_picks(
            records=[self._record_from_dto(movie) for movie in movies],
            limit=self.GENRE_SECTION_LIMIT,
            default_reason=f"{genre} 장르에서 골랐어요",
            default_source="genre_section",
        )
        if not picks:
            return None
        return PersonalizedMovieSection(
            key=f"genre-{genre}",
            title=f"{genre} 장르 예상 픽",
            movies=picks,
        )

    async def _build_genre_section_from_es(
        self,
        *,
        genre: str,
        movies: list[ESSearchMovieItem],
    ) -> PersonalizedMovieSection | None:
        """ES 검색 결과로 장르 섹션 하나를 구성합니다."""
        picks = await self._records_to_preview_picks(
            records=[self._record_from_es_movie(movie) for movie in movies],
            limit=self.GENRE_SECTION_LIMIT,
            default_reason=f"{genre} 장르에서 골랐어요",
            default_source="genre_section_es",
        )
        if not picks:
            return None
        return PersonalizedMovieSection(
            key=f"genre-{genre}",
            title=f"{genre} 장르 예상 픽",
            movies=picks,
        )

    async def _build_wishlist_section_movies(
        self,
        *,
        wishlist_seed_records: list[PersonalizedMovieRecord],
    ) -> list[PersonalizedMoviePick]:
        """위시리스트 섹션용 영화를 구성합니다."""
        return await self._records_to_preview_picks(
            records=wishlist_seed_records,
            limit=self.WISHLIST_SECTION_LIMIT,
            default_reason="위시리스트에 담아둔 영화예요",
            default_source="wishlist_section",
        )

    async def _build_review_sections(
        self,
        *,
        review_rows: list[dict],
        records_by_id: dict[str, PersonalizedMovieRecord],
    ) -> list[PersonalizedMovieSection]:
        """높게 평가한 리뷰 기반 추천 섹션을 구성합니다."""
        section_rows = self._select_review_section_rows(review_rows)[: self.REVIEW_SECTION_COUNT]
        if not section_rows:
            return []

        related_service = self._create_related_movie_service()
        results = await asyncio.gather(
            *[
                self._build_review_section(
                    related_service=related_service,
                    row=row,
                    records_by_id=records_by_id,
                )
                for row in section_rows
            ],
            return_exceptions=True,
        )

        sections: list[PersonalizedMovieSection] = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning("personalized_review_section_failed error=%s", result)
                continue
            if result is not None and result.movies:
                sections.append(result)
        return sections

    async def _build_review_section(
        self,
        *,
        related_service: RelatedMovieService,
        row: dict,
        records_by_id: dict[str, PersonalizedMovieRecord],
    ) -> PersonalizedMovieSection | None:
        """리뷰 하나에 대응하는 related 섹션을 구성합니다."""
        movie_id = str(row.get("movie_id") or "").strip()
        if not movie_id:
            return None

        try:
            response = await related_service.get_related_movies(
                movie_id,
                limit=self.REVIEW_SECTION_LIMIT,
            )
        except Exception as exc:
            logger.warning(
                "personalized_review_related_fetch_failed movie_id=%s error=%s",
                movie_id,
                exc,
            )
            return None

        section_title = str(row.get("movie_title") or "").strip()
        if not section_title:
            section_title = records_by_id.get(movie_id).title if records_by_id.get(movie_id) else movie_id

        movies = self._related_items_to_preview_picks(response.movies)
        if not movies:
            return None
        return PersonalizedMovieSection(
            key=f"review-{movie_id}",
            title=f"{section_title}에 높은 점수를 주셨어요",
            movies=movies,
        )

    async def _build_similar_taste_movies(
        self,
        *,
        favorite_seed_records: list[PersonalizedMovieRecord],
        review_rows: list[dict],
    ) -> list[PersonalizedMoviePick]:
        """비슷한 취향 유저 기반 섹션 영화를 구성합니다."""
        related_service = self._create_related_movie_service()
        seed_ids = self._unique_ordered(
            [record.movie_id for record in favorite_seed_records]
            + [
                str(row.get("movie_id") or "").strip()
                for row in self._select_review_section_rows(review_rows)
            ]
        )[:4]
        if not seed_ids:
            return await self._build_similar_taste_fallback_movies()

        results = await asyncio.gather(
            *[
                related_service.get_related_movies(movie_id, limit=15)
                for movie_id in seed_ids
            ],
            return_exceptions=True,
        )
        merged_items: list = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning("personalized_similar_taste_related_failed error=%s", result)
                continue
            merged_items.extend(result.movies)

        picks = self._merge_related_items_to_preview_picks(
            merged_items,
            limit=self.SIMILAR_TASTE_LIMIT,
        )
        if picks:
            return picks
        return await self._build_similar_taste_fallback_movies()

    async def _build_similar_taste_fallback_movies(self) -> list[PersonalizedMoviePick]:
        """비슷한 취향 섹션이 비었을 때 박스오피스로 폴백합니다."""
        try:
            response = await self._search_service.get_home_box_office_movies(
                page=1,
                size=self.SIMILAR_TASTE_LIMIT,
            )
        except Exception as exc:
            logger.warning("personalized_similar_taste_fallback_failed error=%s", exc)
            return []

        return await self._records_to_preview_picks(
            records=[self._record_from_brief(movie) for movie in response.movies],
            limit=self.SIMILAR_TASTE_LIMIT,
            default_reason="최근 많이 보는 인기작이에요",
            default_source="similar_taste_fallback",
        )

    async def _add_genre_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        cbf_weight: float,
    ) -> None:
        """선호 장르 기반 후보를 누적합니다."""
        target_genres = preferred_genres[: self.PREFERRED_GENRE_LIMIT]
        if not target_genres:
            return

        if self._search_es.is_available():
            results = await asyncio.gather(
                *[
                    self._search_es.search_movies(
                        keyword=None,
                        search_type="title",
                        genre=None,
                        genres=[genre],
                        genre_match_groups=None,
                        year_from=None,
                        year_to=None,
                        rating_min=None,
                        rating_max=None,
                        popularity_min=None,
                        popularity_max=None,
                        vote_count_min=None,
                        sort_by="rating",
                        sort_order="desc",
                        page=1,
                        size=self.GENRE_CANDIDATE_LIMIT,
                    )
                    for genre in target_genres
                ],
                return_exceptions=True,
            )

            for index, genre in enumerate(target_genres):
                genre_weight = max(22.0 - (index * 4.0), 10.0) * cbf_weight
                result = results[index]
                if isinstance(result, Exception):
                    logger.warning(
                        "personalized_top_picks_es_genre_failed genre=%s error=%s",
                        genre,
                        result,
                    )
                    await self._add_genre_candidates_from_db(
                        candidate_map=candidate_map,
                        genre=genre,
                        genre_weight=genre_weight,
                        preferred_genres=preferred_genres,
                        behavior_affinity=behavior_affinity,
                        exclude_ids=exclude_ids,
                    )
                    continue

                if result is None:
                    await self._add_genre_candidates_from_db(
                        candidate_map=candidate_map,
                        genre=genre,
                        genre_weight=genre_weight,
                        preferred_genres=preferred_genres,
                        behavior_affinity=behavior_affinity,
                        exclude_ids=exclude_ids,
                    )
                    continue

                for movie in result.movies:
                    self._add_candidate(
                        candidate_map=candidate_map,
                        record=self._record_from_es_movie(movie),
                        score=genre_weight,
                        source="genre_preference_es",
                        reason=f"선호 장르 {genre} 취향과 잘 맞아요",
                        preferred_genres=preferred_genres,
                        behavior_affinity=behavior_affinity,
                        exclude_ids=exclude_ids,
                    )
            return

        for index, genre in enumerate(target_genres):
            genre_weight = max(22.0 - (index * 4.0), 10.0) * cbf_weight
            await self._add_genre_candidates_from_db(
                candidate_map=candidate_map,
                genre=genre,
                genre_weight=genre_weight,
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
            )

    async def _add_seed_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        seed_records: list[PersonalizedMovieRecord],
        seed_type: str,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        weight: float,
    ) -> None:
        """최애/리뷰/암묵/위시리스트 seed 기반 후보를 누적합니다."""
        if self._search_es.is_available():
            await self._add_seed_candidates_from_es(
                candidate_map=candidate_map,
                seed_records=seed_records,
                seed_type=seed_type,
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
                weight=weight,
            )
            return

        for seed in seed_records:
            await self._add_collection_candidates(
                candidate_map=candidate_map,
                seed=seed,
                seed_type=seed_type,
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
                weight=weight,
            )

    async def _add_seed_candidates_from_es(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        seed_records: list[PersonalizedMovieRecord],
        seed_type: str,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        weight: float,
    ) -> None:
        """seed별 ES related search 한 번으로 후보를 수집합니다."""
        if not seed_records:
            return

        results = await asyncio.gather(
            *[
                self._search_es.search_related_movies(
                    movie_id=seed.movie_id,
                    title=seed.title,
                    title_en=seed.title_en,
                    overview=seed.overview,
                    director=seed.director,
                    cast_members=seed.cast,
                    genres=seed.genres,
                    collection_name=seed.collection_name,
                    limit=max(self.COLLECTION_CANDIDATE_LIMIT, self.DIRECTOR_CANDIDATE_LIMIT),
                )
                for seed in seed_records
            ],
            return_exceptions=True,
        )

        for index, seed in enumerate(seed_records):
            result = results[index]
            if isinstance(result, Exception):
                logger.warning(
                    "personalized_top_picks_es_related_failed seed_type=%s seed_movie_id=%s error=%s",
                    seed_type,
                    seed.movie_id,
                    result,
                )
                await self._add_collection_candidates(
                    candidate_map=candidate_map,
                    seed=seed,
                    seed_type=seed_type,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                    weight=weight,
                )
                await self._add_creator_candidates(
                    candidate_map=candidate_map,
                    seed=seed,
                    seed_type=seed_type,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                    weight=weight,
                )
                continue

            if result is None:
                await self._add_collection_candidates(
                    candidate_map=candidate_map,
                    seed=seed,
                    seed_type=seed_type,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                    weight=weight,
                )
                await self._add_creator_candidates(
                    candidate_map=candidate_map,
                    seed=seed,
                    seed_type=seed_type,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                    weight=weight,
                )
                continue

            score = self._seed_source_weight(seed_type, "related") * weight
            reason = f"{self._seed_reason_prefix(seed_type)} {self._short_title(seed.title)}와 결이 비슷한 작품이에요"
            for movie in result:
                self._add_candidate(
                    candidate_map=candidate_map,
                    record=self._record_from_es_movie(movie),
                    score=score,
                    source=f"{seed_type}_related_es",
                    reason=reason,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                )

    async def _add_genre_candidates_from_db(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        genre: str,
        genre_weight: float,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
    ) -> None:
        """ES 사용 불가 시 기존 MySQL 장르 검색으로 폴백합니다."""
        movies, _ = await self._movie_repo.search(
            genres=[genre],
            sort_by="rating",
            sort_order="desc",
            page=1,
            size=self.GENRE_CANDIDATE_LIMIT,
        )
        for movie in movies:
            self._add_candidate(
                candidate_map=candidate_map,
                record=self._record_from_dto(movie),
                score=genre_weight,
                source="genre_preference",
                reason=f"선호 장르 {genre} 취향과 잘 맞아요",
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
            )

    async def _add_collection_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        seed: PersonalizedMovieRecord,
        seed_type: str,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        weight: float,
    ) -> None:
        """같은 컬렉션 작품 후보를 누적합니다."""
        if not seed.collection_name:
            return

        collection_movies = await self._movie_repo.find_by_collection_name(
            seed.collection_name,
            exclude_movie_id=seed.movie_id,
        )
        base_score = self._seed_source_weight(seed_type, "collection") * weight
        reason = f"{self._seed_reason_prefix(seed_type)} {self._short_title(seed.title)}와 같은 컬렉션이에요"

        for movie in collection_movies[: self.COLLECTION_CANDIDATE_LIMIT]:
            self._add_candidate(
                candidate_map=candidate_map,
                record=self._record_from_dto(movie),
                score=base_score,
                source=f"{seed_type}_collection",
                reason=reason,
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
            )

    async def _add_creator_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        seed: PersonalizedMovieRecord,
        seed_type: str,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        weight: float,
    ) -> None:
        """같은 감독/배우 기반 후보를 누적합니다."""
        if seed.director:
            movies, _ = await self._movie_repo.search(
                keyword=seed.director,
                search_type="director",
                sort_by="rating",
                sort_order="desc",
                page=1,
                size=self.DIRECTOR_CANDIDATE_LIMIT,
            )
            reason = f"{self._seed_reason_prefix(seed_type)} {self._short_title(seed.title)}와 같은 감독 작품이에요"
            for movie in movies:
                self._add_candidate(
                    candidate_map=candidate_map,
                    record=self._record_from_dto(movie),
                    score=self._seed_source_weight(seed_type, "director") * weight,
                    source=f"{seed_type}_director",
                    reason=reason,
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                )

        lead_actor = next(
            (actor for actor in seed.cast if isinstance(actor, str) and actor.strip()),
            None,
        )
        if not lead_actor:
            return

        movies, _ = await self._movie_repo.search(
            keyword=lead_actor,
            search_type="actor",
            sort_by="rating",
            sort_order="desc",
            page=1,
            size=self.ACTOR_CANDIDATE_LIMIT,
        )
        reason = f"{self._seed_reason_prefix(seed_type)} {self._short_title(seed.title)}와 연결되는 배우가 나와요"
        for movie in movies:
            self._add_candidate(
                candidate_map=candidate_map,
                record=self._record_from_dto(movie),
                score=self._seed_source_weight(seed_type, "actor") * weight,
                source=f"{seed_type}_actor",
                reason=reason,
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
            )

    async def _add_cowatched_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        seed_records: list[PersonalizedMovieRecord],
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        cf_weight: float,
    ) -> None:
        """co-watched CF 후보를 누적합니다."""
        unique_seed_records: list[PersonalizedMovieRecord] = []
        seen_seed_ids: set[str] = set()
        for seed in seed_records:
            if seed.movie_id in seen_seed_ids:
                continue
            seen_seed_ids.add(seed.movie_id)
            unique_seed_records.append(seed)

        if len(unique_seed_records) < 2:
            return

        pair_count = 0
        for left, right in combinations(unique_seed_records[:4], 2):
            if pair_count >= self.CO_WATCH_PAIR_LIMIT:
                break
            pair_count += 1
            rows = await self._match_cowatch_service.get_cowatched_candidates(
                movie_id_1=left.movie_id,
                movie_id_2=right.movie_id,
                top_k=self.CO_WATCH_CANDIDATE_LIMIT,
                rating_threshold=self.CO_WATCH_RATING_THRESHOLD,
            )
            if not rows:
                continue

            movie_ids = self._unique_ordered(
                [str(row.get("movie_id")).strip() for row in rows if row.get("movie_id")]
            )
            if not movie_ids:
                continue

            movies = await self._movie_repo.find_by_ids(movie_ids)
            movie_map = {
                record.movie_id: record
                for record in (self._record_from_dto(movie) for movie in movies)
            }

            for row in rows:
                movie_id = str(row.get("movie_id") or "").strip()
                record = movie_map.get(movie_id)
                if record is None:
                    continue

                cf_score = self._coerce_float(row.get("cf_score")) or 0.0
                co_user_count = int(row.get("co_user_count") or 0)
                self._add_candidate(
                    candidate_map=candidate_map,
                    record=record,
                    score=((24.0 * cf_score) + min(co_user_count, 5)) * cf_weight,
                    source="cowatched_cf",
                    reason="비슷한 취향 유저들이 함께 좋아한 작품이에요",
                    preferred_genres=preferred_genres,
                    behavior_affinity=behavior_affinity,
                    exclude_ids=exclude_ids,
                )

    async def _add_box_office_fallback(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
        limit: int,
    ) -> None:
        """박스오피스 fallback 후보를 누적합니다."""
        try:
            response = await self._search_service.get_home_box_office_movies(page=1, size=limit)
        except Exception as exc:
            logger.warning("personalized_top_picks_box_office_fallback_failed error=%s", exc)
            return

        for index, movie in enumerate(response.movies):
            base_score = max(8.0 - (index * 0.25), 1.0)
            self._add_candidate(
                candidate_map=candidate_map,
                record=self._record_from_brief(movie),
                score=base_score,
                source="home_box_office",
                reason="최근 많이 보는 인기작이에요",
                preferred_genres=preferred_genres,
                behavior_affinity=behavior_affinity,
                exclude_ids=exclude_ids,
            )

    def _add_candidate(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        record: PersonalizedMovieRecord,
        score: float,
        source: str,
        reason: str | None,
        preferred_genres: list[str],
        behavior_affinity: dict[str, float],
        exclude_ids: set[str],
    ) -> None:
        """후보 한 편의 점수/사유를 누적합니다."""
        movie_id = str(record.movie_id).strip()
        if not movie_id or movie_id in exclude_ids:
            return

        preferred_matches = set(record.genres) & set(preferred_genres)
        behavior_matches = set(record.genres) & set(behavior_affinity.keys())

        candidate = candidate_map.setdefault(
            movie_id,
            PersonalizedCandidate(movie=record),
        )
        candidate.add(
            score=score,
            source=source,
            reason=reason,
            preferred_genres=preferred_matches,
            behavior_genres=behavior_matches,
        )

    async def _resolve_ranked_candidate_posters(
        self,
        *,
        ranked_candidates: list[PersonalizedCandidate],
        limit: int,
    ) -> list[PersonalizedCandidate]:
        """상위 노출권 후보만 제목 fallback lookup 대상으로 삼아 포스터를 보정합니다."""
        if not ranked_candidates:
            return []

        lookup_window = min(
            max(limit * self.POSTER_LOOKUP_WINDOW_MULTIPLIER, self.POSTER_LOOKUP_MIN_CANDIDATES),
            self.POSTER_LOOKUP_MAX_CANDIDATES,
        )
        title_lookup = await self._build_candidate_title_lookup(
            ranked_candidates=ranked_candidates[:lookup_window],
            max_titles=self.POSTER_LOOKUP_MAX_TITLES,
        )
        resolved_by_movie_id: dict[str, PersonalizedCandidate] = {}
        resolved_candidates: list[PersonalizedCandidate] = []

        for index, candidate in enumerate(ranked_candidates):
            resolved_record = candidate.movie
            if not is_allowed_poster_url(resolved_record.poster_url):
                if index >= lookup_window:
                    continue
                resolved_record = self._select_title_fallback_record(
                    record=resolved_record,
                    title_lookup=title_lookup,
                )
                if resolved_record is None:
                    continue

            target = resolved_by_movie_id.get(resolved_record.movie_id)
            if target is None:
                target = PersonalizedCandidate(
                    movie=resolved_record,
                    source_score=candidate.source_score,
                    final_score=candidate.final_score,
                    reasons=list(candidate.reasons),
                    sources=list(candidate.sources),
                    preferred_genres=set(candidate.preferred_genres),
                    behavior_genres=set(candidate.behavior_genres),
                )
                resolved_by_movie_id[resolved_record.movie_id] = target
                resolved_candidates.append(target)
                continue

            if self._record_quality_score(resolved_record) > self._record_quality_score(target.movie):
                target.movie = resolved_record
            target.source_score += candidate.source_score
            target.final_score = max(target.final_score, candidate.final_score)
            for reason in candidate.reasons:
                if reason not in target.reasons:
                    target.reasons.append(reason)
            for source in candidate.sources:
                if source not in target.sources:
                    target.sources.append(source)
            target.preferred_genres.update(candidate.preferred_genres)
            target.behavior_genres.update(candidate.behavior_genres)

        return resolved_candidates

    async def _build_candidate_title_lookup(
        self,
        *,
        ranked_candidates: list[PersonalizedCandidate],
        max_titles: int = 200,
    ) -> dict[str, list[MovieDTO]]:
        """포스터 보정이 필요한 후보만 모아 제목 exact match lookup을 구성합니다."""
        titles: list[str] = []
        for candidate in ranked_candidates:
            if is_allowed_poster_url(candidate.movie.poster_url):
                continue
            titles.extend(
                collect_exact_title_candidates(
                    candidate.movie.title,
                    candidate.movie.title_en,
                )
            )

        if not titles:
            return {}

        normalized_max_titles = max(1, min(max_titles, 200))
        unique_titles = list(dict.fromkeys(titles))[:normalized_max_titles]
        movies = await self._movie_repo.find_with_posters_by_titles(
            unique_titles,
            limit=min(normalized_max_titles * 2, 200),
        )
        title_lookup: dict[str, dict[str, MovieDTO]] = {}
        for movie in movies:
            for title in collect_exact_title_candidates(movie.title, movie.title_en):
                bucket = title_lookup.setdefault(title, {})
                bucket[movie.movie_id] = movie

        return {
            title: list(bucket.values())
            for title, bucket in title_lookup.items()
        }

    def _select_title_fallback_record(
        self,
        *,
        record: PersonalizedMovieRecord,
        title_lookup: dict[str, list[MovieDTO]],
    ) -> PersonalizedMovieRecord | None:
        """제목 exact match 후보 중 포스터가 있는 영화를 개인화 record로 변환합니다."""
        for candidate_title in collect_exact_title_candidates(record.title, record.title_en):
            for movie in title_lookup.get(candidate_title, []):
                fallback_record = self._record_from_dto(movie)
                if is_allowed_poster_url(fallback_record.poster_url):
                    return fallback_record
        return None

    def _finalize_candidates(
        self,
        *,
        candidate_map: dict[str, PersonalizedCandidate],
        behavior_affinity: dict[str, float],
    ) -> list[PersonalizedCandidate]:
        """후보별 최종 점수를 계산해 내림차순 정렬합니다."""
        ranked_candidates: list[PersonalizedCandidate] = []
        for candidate in candidate_map.values():
            quality_multiplier = self._data_quality_multiplier(candidate.movie)
            rating_boost = min((candidate.movie.rating or 0.0) / 10.0 * 6.0, 6.0)
            vote_boost = min(math.log10((candidate.movie.vote_count or 0) + 1), 3.2)
            multi_source_bonus = max(0, len(candidate.sources) - 1) * 2.5
            preferred_boost = min(len(candidate.preferred_genres) * 3.5, 10.0)
            behavior_boost = min(
                sum(behavior_affinity.get(genre, 0.0) for genre in candidate.behavior_genres) * 6.0,
                8.0,
            )
            candidate.final_score = round(
                (
                    candidate.source_score
                    + rating_boost
                    + vote_boost
                    + multi_source_bonus
                    + preferred_boost
                    + behavior_boost
                ) * quality_multiplier,
                4,
            )
            ranked_candidates.append(candidate)

        ranked_candidates.sort(
            key=lambda candidate: (
                candidate.final_score,
                candidate.movie.rating or 0.0,
                candidate.movie.vote_count or 0,
                candidate.movie.release_year or 0,
            ),
            reverse=True,
        )
        return ranked_candidates

    def _select_ranked_candidates(
        self,
        *,
        ranked_candidates: list[PersonalizedCandidate],
        limit: int,
    ) -> list[PersonalizedCandidate]:
        """관련성과 다양성을 함께 고려해 최종 top picks를 뽑습니다."""
        if not ranked_candidates:
            return []

        popular_candidates = [
            candidate for candidate in ranked_candidates
            if self._is_popular(candidate.movie)
        ]
        hidden_candidates = [
            candidate for candidate in ranked_candidates
            if not self._is_popular(candidate.movie)
        ]

        hidden_slots = 0 if limit <= 4 else min(2, max(1, limit // 4))
        popular_slots = max(limit - hidden_slots, 0)

        selected = self._select_diverse_candidates(popular_candidates, popular_slots)
        selected.extend(
            self._select_diverse_candidates(hidden_candidates, hidden_slots, already_selected=selected)
        )

        if len(selected) < limit:
            selected_ids = {candidate.movie.movie_id for candidate in selected}
            leftovers = [
                candidate
                for candidate in ranked_candidates
                if candidate.movie.movie_id not in selected_ids
            ]
            selected.extend(
                self._select_diverse_candidates(
                    leftovers,
                    limit - len(selected),
                    already_selected=selected,
                )
            )

        return selected[:limit]

    def _select_diverse_candidates(
        self,
        candidates: list[PersonalizedCandidate],
        limit: int,
        *,
        already_selected: list[PersonalizedCandidate] | None = None,
    ) -> list[PersonalizedCandidate]:
        """MMR에 가까운 단순 greedy 방식으로 장르 다양성을 확보합니다."""
        if limit <= 0 or not candidates:
            return []

        selected: list[PersonalizedCandidate] = []
        remaining = list(candidates)
        reference = list(already_selected or [])

        while remaining and len(selected) < limit:
            best_candidate: PersonalizedCandidate | None = None
            best_score = float("-inf")

            for candidate in remaining:
                diversity_penalty = 0.0
                compared = reference + selected
                if compared:
                    diversity_penalty = max(
                        self._genre_similarity(candidate.movie.genres, item.movie.genres)
                        for item in compared
                    )
                score = candidate.final_score - (diversity_penalty * 6.0)
                if score > best_score:
                    best_score = score
                    best_candidate = candidate

            if best_candidate is None:
                break

            selected.append(best_candidate)
            remaining = [
                candidate
                for candidate in remaining
                if candidate.movie.movie_id != best_candidate.movie.movie_id
            ]

        return selected

    def _to_pick(self, candidate: PersonalizedCandidate) -> PersonalizedMoviePick:
        """최종 후보를 API 응답 스키마로 변환합니다."""
        reasons = list(candidate.reasons)
        if candidate.preferred_genres:
            genre_names = sorted(candidate.preferred_genres)
            reasons.append(f"선호 장르 {', '.join(genre_names[:2])}와 잘 맞아요")
        if candidate.behavior_genres and len(reasons) < 3:
            behavior_names = sorted(candidate.behavior_genres)
            reasons.append(f"자주 반응한 {', '.join(behavior_names[:2])} 장르와 결이 맞아요")
        if not reasons:
            reasons.append("현재 취향 신호와 잘 맞는 작품이에요")

        unique_reasons = self._unique_ordered(reasons)[:3]
        movie = candidate.movie
        return PersonalizedMoviePick(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=movie.genres,
            release_year=movie.release_year,
            rating=movie.rating,
            vote_count=movie.vote_count,
            poster_url=movie.poster_url if is_allowed_poster_url(movie.poster_url) else None,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
            personalized_score=round(candidate.final_score, 4),
            personalized_reasons=unique_reasons,
            personalized_sources=candidate.sources,
        )

    def _to_preview_pick(
        self,
        *,
        record: PersonalizedMovieRecord,
        reason: str | None = None,
        source: str | None = None,
    ) -> PersonalizedMoviePick:
        """섹션용 미리보기 영화를 API 응답 스키마로 변환합니다."""
        reasons = [reason] if reason else []
        sources = [source] if source else []
        return PersonalizedMoviePick(
            movie_id=record.movie_id,
            title=record.title,
            title_en=record.title_en,
            genres=record.genres,
            release_year=record.release_year,
            rating=record.rating,
            vote_count=record.vote_count,
            poster_url=record.poster_url if is_allowed_poster_url(record.poster_url) else None,
            trailer_url=record.trailer_url,
            overview=record.overview,
            personalized_score=0.0,
            personalized_reasons=reasons,
            personalized_sources=sources,
        )

    def _create_related_movie_service(self) -> RelatedMovieService:
        """개인화 섹션에서 재사용할 related movie service를 생성합니다."""
        return RelatedMovieService(self._conn, self._redis)

    async def _records_to_preview_picks(
        self,
        *,
        records: list[PersonalizedMovieRecord],
        limit: int,
        default_reason: str | None,
        default_source: str | None,
    ) -> list[PersonalizedMoviePick]:
        """record 목록을 포스터 보정 후 섹션 미리보기 pick 목록으로 변환합니다."""
        if not records or limit <= 0:
            return []

        deduped_records = self._dedupe_records_by_movie_id(records)
        title_lookup = await self._build_preview_title_lookup(deduped_records)
        resolved_records: list[PersonalizedMovieRecord] = []
        for record in deduped_records:
            resolved_record = record
            if not is_allowed_poster_url(record.poster_url):
                fallback_record = self._select_title_fallback_record(
                    record=record,
                    title_lookup=title_lookup,
                )
                if fallback_record is None:
                    continue
                resolved_record = fallback_record
            resolved_records.append(resolved_record)

        return [
            self._to_preview_pick(
                record=record,
                reason=default_reason,
                source=default_source,
            )
            for record in resolved_records[:limit]
        ]

    async def _build_preview_title_lookup(
        self,
        records: list[PersonalizedMovieRecord],
    ) -> dict[str, list[MovieDTO]]:
        """섹션용 preview record의 제목 exact match lookup을 구성합니다."""
        titles: list[str] = []
        for record in records:
            if is_allowed_poster_url(record.poster_url):
                continue
            titles.extend(
                collect_exact_title_candidates(
                    record.title,
                    record.title_en,
                )
            )

        if not titles:
            return {}

        unique_titles = list(dict.fromkeys(titles))[: self.POSTER_LOOKUP_MAX_TITLES]
        movies = await self._movie_repo.find_with_posters_by_titles(
            unique_titles,
            limit=min(len(unique_titles) * 2, 200),
        )
        title_lookup: dict[str, dict[str, MovieDTO]] = {}
        for movie in movies:
            for title in collect_exact_title_candidates(movie.title, movie.title_en):
                bucket = title_lookup.setdefault(title, {})
                bucket[movie.movie_id] = movie
        return {
            title: list(bucket.values())
            for title, bucket in title_lookup.items()
        }

    def _related_items_to_preview_picks(
        self,
        items: list[object],
    ) -> list[PersonalizedMoviePick]:
        """related movie 응답을 섹션용 preview pick 목록으로 변환합니다."""
        picks: list[PersonalizedMoviePick] = []
        seen_movie_ids: set[str] = set()
        for item in items:
            movie_id = str(getattr(item, "movie_id", "") or "").strip()
            if not movie_id or movie_id in seen_movie_ids:
                continue
            seen_movie_ids.add(movie_id)
            record = PersonalizedMovieRecord(
                movie_id=movie_id,
                title=str(getattr(item, "title", "") or "").strip(),
                title_en=getattr(item, "title_en", None),
                genres=list(getattr(item, "genres", []) or []),
                release_year=getattr(item, "release_year", None),
                rating=getattr(item, "rating", None),
                vote_count=getattr(item, "vote_count", None),
                poster_url=getattr(item, "poster_url", None),
                trailer_url=getattr(item, "trailer_url", None),
                overview=getattr(item, "overview", None),
            )
            reasons = list(getattr(item, "relation_reasons", []) or [])
            sources = list(getattr(item, "relation_sources", []) or [])
            picks.append(
                PersonalizedMoviePick(
                    movie_id=record.movie_id,
                    title=record.title,
                    title_en=record.title_en,
                    genres=record.genres,
                    release_year=record.release_year,
                    rating=record.rating,
                    vote_count=record.vote_count,
                    poster_url=record.poster_url if is_allowed_poster_url(record.poster_url) else None,
                    trailer_url=record.trailer_url,
                    overview=record.overview,
                    personalized_score=0.0,
                    personalized_reasons=self._unique_ordered(reasons)[:3],
                    personalized_sources=self._unique_ordered(sources),
                )
            )
        return picks

    def _merge_related_items_to_preview_picks(
        self,
        items: list[object],
        *,
        limit: int,
    ) -> list[PersonalizedMoviePick]:
        """여러 related 응답을 하나의 preview pick 목록으로 합칩니다."""
        return self._related_items_to_preview_picks(items)[:limit]

    def _build_preferred_genres(
        self,
        *,
        favorite_genres: list[dict],
        user_preference_genres: list[str],
        worldcup_genres: list[str],
        behavior_profile: dict,
        seed_records: list[PersonalizedMovieRecord],
    ) -> list[str]:
        """명시 선호 + 행동 프로필 + seed 영화에서 장르 우선순위를 조합합니다."""
        favorite_genre_names = [
            str(item.get("genre_name") or "").strip()
            for item in favorite_genres
            if item.get("genre_name")
        ]
        behavior_genre_names = [
            genre
            for genre, _score in sorted(
                self._normalize_behavior_affinity(behavior_profile.get("genre_affinity")).items(),
                key=lambda item: item[1],
                reverse=True,
            )
        ]
        derived_genre_names = self._collect_top_genres(seed_records, limit=6)
        return self._unique_ordered(
            favorite_genre_names
            + user_preference_genres
            + worldcup_genres
            + behavior_genre_names
            + derived_genre_names
        )[: self.PREFERRED_GENRE_LIMIT]

    @staticmethod
    def _extract_worldcup_genres(worldcup_result: object | None) -> list[str]:
        """월드컵 결과의 장르 선호 JSON에서 상위 장르를 뽑습니다."""
        raw_preferences = getattr(worldcup_result, "genre_preferences", None)
        if not raw_preferences:
            return []

        try:
            parsed_preferences = json.loads(raw_preferences)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []

        if not isinstance(parsed_preferences, dict):
            return []

        ranked_genres = sorted(
            (
                (str(genre or "").strip(), PersonalizedSearchService._coerce_float(score) or 0.0)
                for genre, score in parsed_preferences.items()
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        return [
            genre_name
            for genre_name, score in ranked_genres
            if genre_name and score > 0
        ][:3]

    def _pick_seed_records(
        self,
        ordered_ids: list[str],
        records_by_id: dict[str, PersonalizedMovieRecord],
        *,
        limit: int,
    ) -> list[PersonalizedMovieRecord]:
        """우선순서가 이미 정해진 영화 ID 목록에서 seed record를 추립니다."""
        records: list[PersonalizedMovieRecord] = []
        for movie_id in ordered_ids:
            record = records_by_id.get(movie_id)
            if record is None:
                continue
            records.append(record)
            if len(records) >= limit:
                break
        return records

    def _build_records_by_id_from_dto(
        self,
        movies: list[MovieDTO],
    ) -> dict[str, PersonalizedMovieRecord]:
        """MovieDTO 목록을 ID 기반 record 맵으로 바꿉니다."""
        return {
            record.movie_id: record
            for record in (self._record_from_dto(movie) for movie in movies)
        }

    def _record_from_dto(self, movie: MovieDTO) -> PersonalizedMovieRecord:
        """MovieDTO를 record로 정규화합니다."""
        poster_url = build_tmdb_poster_url(self._settings.TMDB_IMAGE_BASE_URL, movie.poster_path)
        return PersonalizedMovieRecord(
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
            director=movie.director,
            cast=self._normalize_cast_list(movie.get_cast_list()),
            collection_name=movie.collection_name,
        )

    def _record_from_es_movie(self, movie: ESSearchMovieItem) -> PersonalizedMovieRecord:
        """ES 후보 영화를 record로 정규화합니다."""
        poster_url = build_tmdb_poster_url(self._settings.TMDB_IMAGE_BASE_URL, movie.poster_path)
        return PersonalizedMovieRecord(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=list(movie.genres or []),
            release_year=movie.release_year,
            rating=movie.rating,
            vote_count=movie.vote_count,
            poster_url=poster_url,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
            director=movie.director,
            cast=self._normalize_cast_list(list(movie.cast or [])),
            collection_name=movie.collection_name,
        )

    @staticmethod
    def _record_from_brief(movie: MovieBrief) -> PersonalizedMovieRecord:
        """MovieBrief를 record로 정규화합니다."""
        return PersonalizedMovieRecord(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=list(movie.genres or []),
            release_year=movie.release_year,
            rating=movie.rating,
            vote_count=movie.vote_count,
            poster_url=movie.poster_url if is_allowed_poster_url(movie.poster_url) else None,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
        )

    @staticmethod
    def _normalize_behavior_affinity(raw_affinity: object) -> dict[str, float]:
        """행동 프로필 장르 가중치를 0~1 범위로 정규화합니다."""
        if not isinstance(raw_affinity, dict):
            return {}

        numeric_affinity: dict[str, float] = {}
        for genre, raw_value in raw_affinity.items():
            genre_name = str(genre or "").strip()
            value = PersonalizedSearchService._coerce_float(raw_value)
            if not genre_name or value is None or value <= 0:
                continue
            numeric_affinity[genre_name] = value

        if not numeric_affinity:
            return {}

        max_value = max(numeric_affinity.values()) or 1.0
        return {
            genre: round(value / max_value, 4)
            for genre, value in numeric_affinity.items()
        }

    @staticmethod
    def _resolve_signal_weights(profile: dict) -> tuple[float, float]:
        """taste_consistency를 이용해 CBF/CF 비중을 약하게 조정합니다."""
        taste_consistency = PersonalizedSearchService._coerce_float(
            profile.get("taste_consistency") if isinstance(profile, dict) else None
        )
        cbf_weight = 1.0
        cf_weight = 1.0

        if taste_consistency is None:
            return cbf_weight, cf_weight
        if taste_consistency > 0.7:
            return 1.15, 0.9
        if taste_consistency < 0.3:
            return 0.9, 1.15
        return cbf_weight, cf_weight

    @staticmethod
    def _collect_top_genres(
        records: list[PersonalizedMovieRecord],
        *,
        limit: int,
    ) -> list[str]:
        """seed 영화의 장르 빈도를 집계해 상위 장르를 뽑습니다."""
        genre_counts: dict[str, int] = {}
        for record in records:
            for genre in record.genres:
                genre_name = str(genre or "").strip()
                if not genre_name:
                    continue
                genre_counts[genre_name] = genre_counts.get(genre_name, 0) + 1

        return [
            genre
            for genre, _count in sorted(
                genre_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[:limit]
        ]

    @staticmethod
    def _select_positive_review_rows(review_rows: list[dict]) -> list[dict]:
        """높게 평가한 리뷰를 우선 seed로 사용합니다."""
        sorted_rows = sorted(
            review_rows,
            key=lambda item: (
                -(PersonalizedSearchService._coerce_float(item.get("rating")) or 0.0),
                str(item.get("created_at") or ""),
            ),
        )
        positive_rows = [
            row
            for row in sorted_rows
            if (PersonalizedSearchService._coerce_float(row.get("rating")) or 0.0)
            >= PersonalizedSearchService.REVIEW_POSITIVE_THRESHOLD
        ]
        return PersonalizedSearchService._dedupe_rows_by_movie_id(positive_rows)[: PersonalizedSearchService.REVIEW_SEED_LIMIT]

    @staticmethod
    def _dedupe_rows_by_movie_id(rows: list[dict]) -> list[dict]:
        """movie_id 기준으로 첫 항목만 남깁니다."""
        deduped: list[dict] = []
        seen_movie_ids: set[str] = set()
        for row in rows:
            movie_id = str(row.get("movie_id") or "").strip()
            if not movie_id or movie_id in seen_movie_ids:
                continue
            seen_movie_ids.add(movie_id)
            deduped.append(row)
        return deduped

    @classmethod
    def _select_review_section_rows(cls, review_rows: list[dict]) -> list[dict]:
        """리뷰 기반 섹션용 seed 리뷰를 추립니다."""
        sorted_rows = sorted(
            review_rows,
            key=lambda item: (
                -(cls._coerce_float(item.get("rating")) or 0.0),
                str(item.get("created_at") or ""),
            ),
        )
        prioritized_rows = [
            *[
                row for row in sorted_rows
                if (cls._coerce_float(row.get("rating")) or 0.0) >= cls.REVIEW_POSITIVE_THRESHOLD
            ],
            *sorted_rows,
        ]
        return cls._dedupe_rows_by_movie_id(prioritized_rows)

    @staticmethod
    def _dedupe_records_by_movie_id(
        records: list[PersonalizedMovieRecord],
    ) -> list[PersonalizedMovieRecord]:
        """record 목록에서 movie_id 기준 첫 항목만 남깁니다."""
        deduped: list[PersonalizedMovieRecord] = []
        seen_movie_ids: set[str] = set()
        for record in records:
            movie_id = str(record.movie_id or "").strip()
            if not movie_id or movie_id in seen_movie_ids:
                continue
            seen_movie_ids.add(movie_id)
            deduped.append(record)
        return deduped

    @staticmethod
    def _data_quality_multiplier(movie: PersonalizedMovieRecord) -> float:
        """포스터/줄거리/평점 충실도에 따른 점수 보정 계수입니다."""
        fields = 0
        if movie.poster_url:
            fields += 1
        if movie.overview and len(movie.overview.strip()) >= 20:
            fields += 1
        if movie.rating and movie.rating >= 1.0:
            fields += 1
        return {3: 1.0, 2: 0.88, 1: 0.72, 0: 0.5}.get(fields, 0.5)

    @staticmethod
    def _record_quality_score(movie: PersonalizedMovieRecord) -> int:
        """같은 영화 ID로 합쳐질 때 더 풍부한 메타데이터 record를 고릅니다."""
        score = 0
        if is_allowed_poster_url(movie.poster_url):
            score += 10
        score += min(len(movie.genres), 3) * 2
        if movie.overview and len(movie.overview.strip()) >= 20:
            score += 4
        if movie.rating is not None:
            score += 2
        if movie.vote_count:
            score += 2
        if movie.director:
            score += 1
        if movie.cast:
            score += 1
        if movie.collection_name:
            score += 1
        return score

    @staticmethod
    def _normalize_cast_list(cast_values: list[object]) -> list[str]:
        """배우 목록을 검색 가능한 문자열 배열로 정규화합니다."""
        normalized_cast: list[str] = []
        seen_names: set[str] = set()
        for value in cast_values:
            actor_name = ""
            if isinstance(value, str):
                actor_name = value.strip()
            elif isinstance(value, dict):
                actor_name = str(value.get("name") or "").strip()
            if not actor_name or actor_name in seen_names:
                continue
            seen_names.add(actor_name)
            normalized_cast.append(actor_name)
        return normalized_cast

    @classmethod
    def _is_popular(cls, movie: PersonalizedMovieRecord) -> bool:
        """검증된 인기작 여부를 판정합니다."""
        rating_ok = bool(movie.rating and movie.rating >= cls.POPULAR_MIN_RATING)
        vote_ok = bool(
            movie.vote_count is not None and movie.vote_count >= cls.POPULAR_MIN_VOTE_COUNT
        )
        return rating_ok or vote_ok

    @staticmethod
    def _genre_similarity(left: list[str], right: list[str]) -> float:
        """장르 Jaccard 유사도를 계산합니다."""
        left_set = {genre for genre in left if genre}
        right_set = {genre for genre in right if genre}
        if not left_set or not right_set:
            return 0.0
        intersection = len(left_set & right_set)
        union = len(left_set | right_set)
        return intersection / union if union else 0.0

    @staticmethod
    def _seed_source_weight(seed_type: str, source_type: str) -> float:
        """seed 강도별 기본 가중치."""
        weights = {
            "favorite": {"collection": 26.0, "director": 17.0, "actor": 12.0, "related": 24.0},
            "worldcup": {"collection": 23.0, "director": 14.0, "actor": 10.0, "related": 20.0},
            "review": {"collection": 24.0, "director": 15.0, "actor": 11.0, "related": 22.0},
            "implicit": {"collection": 20.0, "director": 12.0, "actor": 9.0, "related": 18.0},
            "wishlist": {"collection": 15.0, "director": 9.0, "actor": 7.0, "related": 13.0},
        }
        return weights.get(seed_type, {}).get(source_type, 8.0)

    @staticmethod
    def _seed_reason_prefix(seed_type: str) -> str:
        """seed 타입별 설명용 접두어."""
        return {
            "favorite": "최애 영화",
            "worldcup": "영화 월드컵에서 고른 작품",
            "review": "높게 평가한 작품",
            "implicit": "최근 강하게 반응한 작품",
            "wishlist": "위시리스트에 담은 작품",
        }.get(seed_type, "취향 영화")

    @staticmethod
    def _short_title(title: str | None, max_length: int = 18) -> str:
        """긴 제목을 추천 사유 문장에 맞게 축약합니다."""
        normalized = str(title or "").strip()
        if len(normalized) <= max_length:
            return normalized
        return f"{normalized[: max_length - 1]}…"

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        """숫자형 값을 안전하게 float로 변환합니다."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _unique_ordered(values: list[str]) -> list[str]:
        """순서를 유지한 채 중복을 제거합니다."""
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @classmethod
    def _cache_key(cls, *, user_id: str, limit: int) -> str:
        """개인화 TOP picks 캐시 키를 생성합니다."""
        return f"{cls.CACHE_PREFIX}:{cls.CACHE_VERSION}:{user_id}:limit:{limit}"

    async def _read_cached_top_picks(
        self,
        cache_key: str,
    ) -> PersonalizedTopPicksResponse | None:
        """Redis 캐시 hit 시 개인화 추천 응답을 복원합니다."""
        if self._redis is None:
            return None

        try:
            cached = await self._redis.get(cache_key)
            if not cached:
                return None
            return PersonalizedTopPicksResponse.model_validate_json(cached)
        except Exception as exc:
            logger.warning("personalized_top_picks_cache_read_error key=%s error=%s", cache_key, exc)
            return None

    async def _write_cached_top_picks(
        self,
        cache_key: str,
        response: PersonalizedTopPicksResponse,
    ) -> None:
        """개인화 추천 응답을 Redis에 best-effort 저장합니다."""
        if self._redis is None:
            return

        try:
            await self._redis.setex(
                cache_key,
                self.CACHE_TTL_SECONDS,
                response.model_dump_json(),
            )
        except Exception as exc:
            logger.warning("personalized_top_picks_cache_write_error key=%s error=%s", cache_key, exc)
