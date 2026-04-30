"""
영화 검색 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 SearchService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import logging
from datetime import date, datetime
from math import ceil

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    MovieBrief,
    MovieDetailResponse,
    MovieSearchResponse,
    RecentSearchPagination,
    PaginationMeta,
    RecentSearchItem,
    RecentSearchResponse,
    SearchClickLogResponse,
)
from app.search_elasticsearch import ESSearchMovieItem, ElasticsearchSearchClient
from app.search_genre_catalog import (
    expand_search_genre_aliases,
    get_search_genre_alias_groups,
    normalize_search_genre_labels,
)
from app.v2.model.dto import MovieDTO
from app.v2.repository.movie_repository import MovieRepository
from app.v2.repository.search_history_repository import SearchHistoryRepository
from app.v2.repository.trending_repository import TrendingRepository

logger = logging.getLogger(__name__)


class MovieDetailNotFoundError(LookupError):
    """영화 상세 조회 시 대상을 찾지 못한 경우의 도메인 예외입니다."""


class SearchService:
    """영화 검색 비즈니스 로직 서비스 (v2 Raw SQL)"""

    HOME_BOX_OFFICE_CACHE_KEY_PREFIX = "home_box_office:v2:"

    def __init__(self, conn: aiomysql.Connection, redis_client: aioredis.Redis | None = None):
        """
        Args:
            conn: aiomysql 비동기 커넥션
            redis_client: Redis 비동기 클라이언트 (상세 조회 전용 사용 시 None 가능)
        """
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(conn)
        self._history_repo = SearchHistoryRepository(conn)
        self._trending_repo = TrendingRepository(conn)
        self._search_es = ElasticsearchSearchClient()

    async def search_movies(
        self,
        keyword: str | None = None,
        search_type: str = "title",
        genre: str | None = None,
        genres: list[str] | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        rating_min: float | None = None,
        rating_max: float | None = None,
        popularity_min: float | None = None,
        popularity_max: float | None = None,
        sort_by: str = "rating",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
        user_id: str | None = None,
        save_history: bool = False,
    ) -> MovieSearchResponse:
        """
        영화를 검색하고 필터링된 결과를 반환합니다.

        검색과 동시에 부수 작업 수행:
        - 로그인 사용자 + save_history=true: 검색 이력 저장
        - 인기 검색어 점수 증가 (Redis + MySQL)
        """
        # 입력값 정규화
        page = max(1, page)
        size = min(max(1, size), 100)
        keyword_cleaned = keyword.strip() if keyword and keyword.strip() else None
        selected_genres = normalize_search_genre_labels(genres)
        selected_genre_alias_groups = get_search_genre_alias_groups(selected_genres)
        expanded_genres = expand_search_genre_aliases(selected_genres)
        is_genre_discovery_search = keyword_cleaned is None and bool(selected_genres)
        search_history_keyword = (
            keyword_cleaned if keyword_cleaned is not None else ",".join(selected_genres)
        ) or None
        genre_discovery_vote_count_min = (
            self._settings.GENRE_DISCOVERY_MIN_VOTE_COUNT
            if is_genre_discovery_search and sort_by == "rating"
            else None
        )
        db_sort_by = "relevance" if sort_by == "relevance" else sort_by
        did_you_mean: str | None = None
        related_queries: list[str] = []
        search_source: str | None = None

        es_movies: list[ESSearchMovieItem] | None = None
        total = 0
        if keyword_cleaned is not None or is_genre_discovery_search:
            es_result = await self._search_es.search_movies(
                keyword=keyword_cleaned,
                search_type=search_type,
                genre=genre if keyword_cleaned is not None else None,
                genres=selected_genres if is_genre_discovery_search else None,
                genre_match_groups=selected_genre_alias_groups if is_genre_discovery_search else None,
                year_from=year_from,
                year_to=year_to,
                rating_min=rating_min,
                rating_max=rating_max,
                popularity_min=popularity_min,
                popularity_max=popularity_max,
                vote_count_min=genre_discovery_vote_count_min,
                sort_by=db_sort_by,
                sort_order=sort_order,
                page=page,
                size=size,
            )
            if es_result is not None:
                es_movies = es_result.movies
                total = es_result.total
                did_you_mean = es_result.did_you_mean
                related_queries = es_result.related_queries
                search_source = "elasticsearch"

        if es_movies is None:
            movies, total = await self._movie_repo.search(
                keyword=keyword_cleaned,
                search_type=search_type,
                genre=genre,
                genres=expanded_genres if is_genre_discovery_search else None,
                genre_match_groups=selected_genre_alias_groups if is_genre_discovery_search else None,
                year_from=year_from,
                year_to=year_to,
                rating_min=rating_min,
                rating_max=rating_max,
                popularity_min=popularity_min,
                popularity_max=popularity_max,
                vote_count_min=genre_discovery_vote_count_min,
                sort_by=db_sort_by,
                sort_order=sort_order,
                page=page,
                size=size,
            )
            search_source = "mysql"
        else:
            movies = []

        # 부수 작업: 검색 이력 + 인기 검색어 갱신
        if search_history_keyword:
            should_track_search_event = page == 1

            # 로그인 사용자의 검색 이력 저장
            if user_id and save_history and should_track_search_event:
                try:
                    await self._history_repo.add_search(
                        user_id=user_id,
                        keyword=search_history_keyword,
                        result_count=total,
                        filters=self._build_search_filters(
                            search_mode="genre_discovery" if is_genre_discovery_search else "keyword",
                            search_type=search_type,
                            genre=genre,
                            genres=selected_genres,
                            year_from=year_from,
                            year_to=year_to,
                            rating_min=rating_min,
                            rating_max=rating_max,
                            popularity_min=popularity_min,
                            popularity_max=popularity_max,
                            sort_by=sort_by,
                            sort_order=sort_order,
                            page=page,
                            size=size,
                        ),
                    )
                except Exception as e:
                    logger.warning(f"검색 이력 저장 실패 (user_id={user_id}): {e}")

            # Redis 인기 검색어 점수 증가
            if self._redis is not None and keyword_cleaned is not None:
                try:
                    await self._redis.zincrby("trending:keywords", 1, keyword_cleaned)
                except Exception as e:
                    logger.warning(f"Redis 인기 검색어 갱신 실패: {e}")

            # MySQL 인기 검색어 백업
            if keyword_cleaned is not None:
                try:
                    await self._trending_repo.increment(keyword_cleaned)
                except Exception as e:
                    logger.warning(f"MySQL 인기 검색어 저장 실패: {e}")

        # 응답 변환
        movie_briefs = (
            [self._to_movie_brief(m) for m in movies]
            if es_movies is None
            else [self._to_movie_brief_from_es(m) for m in es_movies]
        )
        total_pages = ceil(total / size) if total > 0 else 0

        return MovieSearchResponse(
            movies=movie_briefs,
            pagination=PaginationMeta(
                page=page,
                size=size,
                total=total,
                total_pages=total_pages,
            ),
            did_you_mean=did_you_mean,
            related_queries=related_queries,
            search_source=search_source,
        )

    async def get_home_box_office_movies(
        self,
        *,
        page: int = 1,
        size: int = 12,
    ) -> MovieSearchResponse:
        """
        홈 인기 영화 섹션용 박스오피스 영화를 반환합니다.

        최신 `box_office_daily.target_dt`부터 과거로 내려가며 영화를 모으고,
        재개봉/중복 적재 레코드는 더 풍부한 원본 영화 레코드로 치환합니다.
        """
        normalized_page = max(1, page)
        normalized_size = min(max(1, size), 30)
        cache_key = self._home_box_office_cache_key(normalized_page, normalized_size)

        cached_response = await self._read_cached_home_box_office_movies(cache_key)
        if cached_response is not None:
            return cached_response

        required_unique_count = normalized_page * normalized_size
        source_limit = max(required_unique_count * 8, 120)

        source_movies = await self._movie_repo.find_home_box_office_source_movies(limit=source_limit)
        candidate_lookup = await self._build_home_box_office_candidate_lookup(source_movies)
        title_lookup = await self._build_home_box_office_title_lookup(source_movies)

        unique_display_movies: list[MovieDTO] = []
        seen_movie_ids: set[str] = set()

        for source_movie in source_movies:
            display_movie = self._resolve_home_box_office_display_movie(
                source_movie,
                candidate_lookup=candidate_lookup,
                title_lookup=title_lookup,
            )
            if display_movie.movie_id in seen_movie_ids:
                continue

            unique_display_movies.append(display_movie)
            seen_movie_ids.add(display_movie.movie_id)

            if len(unique_display_movies) >= required_unique_count:
                break

        start = (normalized_page - 1) * normalized_size
        end = start + normalized_size
        page_movies = unique_display_movies[start:end]
        total = len(unique_display_movies)
        total_pages = ceil(total / normalized_size) if total > 0 else 0

        response = MovieSearchResponse(
            movies=[self._to_movie_brief(movie) for movie in page_movies],
            pagination=PaginationMeta(
                page=normalized_page,
                size=normalized_size,
                total=total,
                total_pages=total_pages,
            ),
            did_you_mean=None,
            related_queries=[],
            search_source="mysql",
        )
        await self._write_cached_home_box_office_movies(cache_key, response)
        return response

    async def get_recent_searches(
        self,
        user_id: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> RecentSearchResponse:
        """사용자의 최근 검색어를 반환합니다."""
        page_limit = min(limit or self._settings.RECENT_SEARCH_MAX, self._settings.RECENT_SEARCH_MAX)
        records, has_more = await self._history_repo.get_recent_page(
            user_id=user_id,
            offset=offset,
            limit=page_limit,
        )
        items = [
            RecentSearchItem(
                keyword=r.keyword,
                searched_at=r.searched_at,
                filters=self._normalize_recent_filters(r.filters),
            )
            for r in records
        ]
        next_offset = offset + len(items) if has_more else None
        return RecentSearchResponse(
            searches=items,
            pagination=RecentSearchPagination(
                offset=offset,
                limit=page_limit,
                has_more=has_more,
                next_offset=next_offset,
            ),
        )

    def _normalize_recent_filters(self, filters) -> dict | None:
        """최근 검색 응답에서 filters가 dict가 아닐 경우 안전하게 제거합니다."""
        return filters if isinstance(filters, dict) else None

    async def log_search_click(
        self,
        user_id: str | None,
        keyword: str,
        clicked_movie_id: str,
        result_count: int,
        filters: dict | None = None,
    ) -> SearchClickLogResponse:
        """검색 결과 클릭 이벤트를 저장합니다."""
        if not user_id:
            return SearchClickLogResponse(
                saved=False,
                message="비로그인 사용자는 검색 클릭 이력을 저장하지 않습니다.",
            )

        await self._history_repo.add_search(
            user_id=user_id,
            keyword=keyword,
            result_count=result_count,
            filters=filters,
            clicked_movie_id=clicked_movie_id,
        )
        return SearchClickLogResponse(
            saved=True,
            message="검색 결과 클릭 이력이 저장되었습니다.",
        )

    async def get_movie_detail(self, movie_id: str) -> MovieDetailResponse:
        """영화 상세 정보를 반환합니다."""
        movie = await self._movie_repo.find_by_id(movie_id)
        if movie is None:
            logger.warning(
                "[v2] movie detail not found movie_id=%s db=%s host=%s",
                movie_id,
                self._settings.DB_NAME,
                self._settings.DB_HOST,
            )
            raise MovieDetailNotFoundError(f"영화 ID '{movie_id}'를 찾을 수 없습니다.")
        return self._to_movie_detail(movie)

    async def delete_recent_keyword(self, user_id: str, keyword: str) -> bool:
        """특정 검색어를 최근 검색 이력에서 삭제합니다."""
        return await self._history_repo.delete_keyword(user_id, keyword)

    async def delete_all_recent(self, user_id: str) -> int:
        """사용자의 모든 최근 검색 이력을 삭제합니다."""
        return await self._history_repo.delete_all(user_id)

    def _build_search_filters(
        self,
        *,
        search_mode: str,
        search_type: str,
        genre: str | None,
        genres: list[str] | None,
        year_from: int | None,
        year_to: int | None,
        rating_min: float | None,
        rating_max: float | None,
        popularity_min: float | None,
        popularity_max: float | None,
        sort_by: str,
        sort_order: str,
        page: int,
        size: int,
    ) -> dict:
        """검색 시 적용한 조건을 직렬화 가능한 dict로 정리합니다."""
        return {
            "search_mode": search_mode,
            "search_type": search_type,
            "genre": genre,
            "genres": genres or [],
            "year_from": year_from,
            "year_to": year_to,
            "rating_min": rating_min,
            "rating_max": rating_max,
            "popularity_min": popularity_min,
            "popularity_max": popularity_max,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "page": page,
            "size": size,
        }

    def _to_movie_brief(self, movie: MovieDTO) -> MovieBrief:
        """MovieDTO를 MovieBrief 스키마로 변환합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        return MovieBrief(
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
        )

    def _to_movie_brief_from_es(self, movie: ESSearchMovieItem) -> MovieBrief:
        """ES 검색 결과 문서를 기존 검색 응답 스키마에 맞춰 정규화합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        return MovieBrief(
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
        )

    def _to_movie_detail(self, movie: MovieDTO) -> MovieDetailResponse:
        """MovieDTO를 MovieDetailResponse로 변환합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        backdrop_url = None
        if movie.backdrop_path:
            backdrop_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.backdrop_path}"

        kobis_open_dt = self._normalize_kobis_open_dt(movie.kobis_open_dt)
        release_date = None
        if kobis_open_dt and len(kobis_open_dt) == 8 and kobis_open_dt.isdigit():
            release_date = (
                f"{kobis_open_dt[:4]}-{kobis_open_dt[4:6]}-{kobis_open_dt[6:8]}"
            )
        elif movie.release_year:
            release_date = f"{movie.release_year}-01-01"

        return MovieDetailResponse(
            movie_id=movie.movie_id,
            title=movie.title,
            original_title=movie.title_en,
            genres=movie.get_genres_list(),
            release_year=movie.release_year,
            release_date=release_date,
            runtime=movie.runtime,
            rating=movie.rating,
            vote_count=movie.vote_count,
            popularity_score=movie.popularity_score,
            poster_url=poster_url,
            backdrop_url=backdrop_url,
            director=movie.director,
            cast=movie.get_cast_list(),
            certification=movie.certification,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
            tagline=movie.tagline,
            imdb_id=movie.imdb_id,
            original_language=movie.original_language,
            collection_name=movie.collection_name,
            kobis_open_dt=kobis_open_dt,
            awards=movie.awards,
            filming_location=movie.filming_location,
            source=movie.source,
        )

    @staticmethod
    def _normalize_kobis_open_dt(value: object) -> str | None:
        """KOBIS 개봉일 값을 YYYYMMDD 문자열로 정규화합니다."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            return value.strip() or None
        return str(value)

    @classmethod
    def _home_box_office_cache_key(cls, page: int, size: int) -> str:
        """홈 인기 영화 박스오피스 캐시 키를 생성합니다."""
        return f"{cls.HOME_BOX_OFFICE_CACHE_KEY_PREFIX}page:{page}:size:{size}"

    async def _read_cached_home_box_office_movies(
        self,
        cache_key: str,
    ) -> MovieSearchResponse | None:
        """Redis에 저장된 홈 박스오피스 응답을 복원합니다."""
        if self._redis is None:
            return None

        try:
            cached = await self._redis.get(cache_key)
            if not cached:
                return None
            return MovieSearchResponse.model_validate_json(cached)
        except Exception as exc:
            logger.warning("home_box_office_cache_read_error key=%s error=%s", cache_key, exc)
            return None

    async def _write_cached_home_box_office_movies(
        self,
        cache_key: str,
        response: MovieSearchResponse,
    ) -> None:
        """홈 박스오피스 응답을 Redis에 best-effort로 저장합니다."""
        if self._redis is None:
            return

        try:
            await self._redis.setex(
                cache_key,
                self._settings.HOME_BOX_OFFICE_CACHE_TTL,
                response.model_dump_json(),
            )
        except Exception as exc:
            logger.warning("home_box_office_cache_write_error key=%s error=%s", cache_key, exc)

    async def _build_home_box_office_candidate_lookup(
        self,
        source_movies: list[MovieDTO],
    ) -> dict[str, list[MovieDTO]]:
        """식별자 기준 후보 영화를 한 번에 조회해 lookup 맵으로 구성합니다."""
        identifiers: list[str] = []
        for source_movie in source_movies:
            identifiers.extend(self._build_movie_identifiers(source_movie))

        candidates = await self._movie_repo.find_by_identifiers(identifiers)
        lookup: dict[str, dict[str, MovieDTO]] = {}

        for candidate in candidates:
            for identifier in self._build_movie_identifiers(candidate):
                bucket = lookup.setdefault(identifier, {})
                bucket[candidate.movie_id] = candidate

        return {
            identifier: list(bucket.values())
            for identifier, bucket in lookup.items()
        }

    async def _build_home_box_office_title_lookup(
        self,
        source_movies: list[MovieDTO],
    ) -> dict[str, list[MovieDTO]]:
        """제목 exact match fallback 후보를 한 번에 조회해 lookup 맵으로 구성합니다."""
        titles: list[str] = []
        for movie in source_movies:
            if movie.title and movie.title.strip():
                titles.append(movie.title.strip())
            if movie.title_en and movie.title_en.strip():
                titles.append(movie.title_en.strip())

        candidates = await self._movie_repo.find_with_posters_by_titles(titles)
        lookup: dict[str, dict[str, MovieDTO]] = {}

        for candidate in candidates:
            for title in (candidate.title, candidate.title_en):
                if not title or not title.strip():
                    continue
                normalized_title = title.strip()
                bucket = lookup.setdefault(normalized_title, {})
                bucket[candidate.movie_id] = candidate

        return {
            title: list(bucket.values())
            for title, bucket in lookup.items()
        }

    def _resolve_home_box_office_display_movie(
        self,
        source_movie: MovieDTO,
        *,
        candidate_lookup: dict[str, list[MovieDTO]],
        title_lookup: dict[str, list[MovieDTO]],
    ) -> MovieDTO:
        """재개봉/중복 적재 후보 중 홈 카드에 가장 적합한 대표 레코드를 선택합니다."""
        candidates = self._collect_candidates_by_identifiers(
            self._build_movie_identifiers(source_movie),
            candidate_lookup,
        )
        best_movie = max(
            candidates or [source_movie],
            key=lambda movie: (
                self._home_box_office_display_score(movie),
                movie.vote_count or 0,
                movie.rating or 0.0,
                movie.release_year or 0,
            ),
        )

        # 강한 식별자 매칭으로도 포스터가 없으면 제목 exact match 후보를 2차 fallback으로 사용한다.
        if not best_movie.poster_path:
            title_candidates = self._collect_candidates_by_titles(
                source_movie.title,
                source_movie.title_en,
                title_lookup,
            )
            if title_candidates:
                merged_candidates: dict[str, MovieDTO] = {
                    movie.movie_id: movie for movie in (candidates or [source_movie])
                }
                for candidate in title_candidates:
                    merged_candidates.setdefault(candidate.movie_id, candidate)

                best_movie = max(
                    merged_candidates.values(),
                    key=lambda movie: (
                        self._home_box_office_display_score(movie),
                        movie.vote_count or 0,
                        movie.rating or 0.0,
                        movie.release_year or 0,
                    ),
                )

        return best_movie

    @staticmethod
    def _collect_candidates_by_identifiers(
        identifiers: list[str],
        candidate_lookup: dict[str, list[MovieDTO]],
    ) -> list[MovieDTO]:
        """식별자 lookup에서 후보 영화를 dedupe하여 반환합니다."""
        merged: dict[str, MovieDTO] = {}
        for identifier in identifiers:
            for candidate in candidate_lookup.get(identifier, []):
                merged[candidate.movie_id] = candidate
        return list(merged.values())

    @staticmethod
    def _collect_candidates_by_titles(
        title: str | None,
        title_en: str | None,
        title_lookup: dict[str, list[MovieDTO]],
    ) -> list[MovieDTO]:
        """제목 lookup에서 후보 영화를 dedupe하여 반환합니다."""
        merged: dict[str, MovieDTO] = {}
        for raw_title in (title, title_en):
            normalized_title = raw_title.strip() if raw_title and raw_title.strip() else None
            if not normalized_title:
                continue
            for candidate in title_lookup.get(normalized_title, []):
                merged[candidate.movie_id] = candidate
        return list(merged.values())

    @staticmethod
    def _build_movie_identifiers(movie: MovieDTO) -> list[str]:
        """같은 영화 후보를 찾기 위한 식별자 목록을 구성합니다."""
        identifiers: list[str] = [movie.movie_id]

        if movie.tmdb_id is not None:
            identifiers.append(str(movie.tmdb_id))
        if movie.imdb_id:
            identifiers.append(movie.imdb_id)
        if movie.kobis_movie_cd:
            identifiers.append(movie.kobis_movie_cd)
        if movie.kmdb_id:
            identifiers.append(movie.kmdb_id)

        return list(dict.fromkeys([identifier.strip() for identifier in identifiers if identifier and identifier.strip()]))

    @staticmethod
    def _home_box_office_display_score(movie: MovieDTO) -> int:
        """홈 카드/상세 진입 품질 기준의 메타데이터 점수를 계산합니다."""
        score = 0

        if movie.poster_path:
            score += 100
        if movie.overview and movie.overview.strip():
            score += 40
        if movie.backdrop_path:
            score += 12
        if movie.genres:
            score += 8
        if movie.rating is not None:
            score += 8
        if movie.vote_count:
            score += 6
        if movie.kobis_open_dt:
            score += 4
        if movie.release_year is not None:
            score += 2

        source = (movie.source or "").strip().lower()
        if source == "tmdb":
            score += 5
        elif source == "kmdb":
            score += 3
        elif source == "kobis":
            score += 2
        elif source == "kaggle":
            score += 1

        return score
