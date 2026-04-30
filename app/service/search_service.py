"""
영화 검색 서비스

REQ_031: 영화 제목/감독/배우로 검색
REQ_034: 검색 결과 상세 필터링 (장르, 연도, 평점, 국가, 정렬)

검색 흐름:
1. 키워드 + 필터 조건으로 MySQL 쿼리 생성 (MovieRepository)
2. 결과를 Pydantic 스키마로 변환
3. 검색 이력 저장 (SearchHistoryRepository)
4. 인기 검색어 점수 증가 (Redis + TrendingRepository)

성능 최적화:
- 페이지네이션: OFFSET + LIMIT (COUNT 별도 쿼리)
- 인기 검색어: Redis Sorted Set으로 실시간 순위 (MySQL은 백업)
"""

import json
import logging
from datetime import date, datetime
from math import ceil

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.entity import Movie
from app.model.schema import (
    MovieBrief,
    SearchClickLogResponse,
    MovieDetailResponse,
    MovieSearchResponse,
    PaginationMeta,
    RecentSearchItem,
    RecentSearchPagination,
    RecentSearchResponse,
)
from app.repository.movie_repository import MovieRepository
from app.repository.search_history_repository import SearchHistoryRepository
from app.repository.trending_repository import TrendingRepository
from app.search_elasticsearch import ESSearchMovieItem, ElasticsearchSearchClient
from app.search_genre_catalog import (
    expand_search_genre_aliases,
    get_search_genre_alias_groups,
    normalize_search_genre_labels,
)

logger = logging.getLogger(__name__)


class MovieDetailNotFoundError(LookupError):
    """영화 상세 조회 시 대상을 찾지 못한 경우의 도메인 예외입니다."""


class SearchService:
    """영화 검색 비즈니스 로직 서비스"""

    def __init__(self, session: AsyncSession, redis_client: aioredis.Redis | None = None):
        """
        Args:
            session: SQLAlchemy 비동기 세션
            redis_client: Redis 비동기 클라이언트 (상세 조회 전용 사용 시 None 가능)
        """
        self._session = session
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(session)
        self._history_repo = SearchHistoryRepository(session)
        self._trending_repo = TrendingRepository(session)
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

        검색과 동시에 다음 부수 작업을 수행합니다:
        - 로그인 사용자 + save_history=true: 검색 이력 저장
        - 인기 검색어 점수 증가 (Redis Sorted Set)
        - 인기 검색어 MySQL 백업 (TrendingKeyword)

        Args:
            keyword: 검색 키워드
            search_type: 검색 대상 ("title", "director", "actor", "all")
            genre: 장르 필터
            genres: 장르만 검색할 때 사용하는 다중 선택 장르 라벨 목록
            year_from: 시작 연도
            year_to: 끝 연도
            rating_min: 최소 평점
            rating_max: 최대 평점
            popularity_min: 최소 인기도
            popularity_max: 최대 인기도
            sort_by: 정렬 기준 ("relevance", "rating", "release_date", "title")
            sort_order: 정렬 방향 ("asc", "desc")
            page: 페이지 번호 (1부터)
            size: 페이지 크기
            user_id: 로그인 사용자 ID (None이면 비로그인)
            save_history: search_history 저장 여부 (/search 페이지 검색만 true)

        Returns:
            MovieSearchResponse: 검색 결과 + 페이지네이션
        """
        # 입력값 정규화
        page = max(1, page)
        size = min(max(1, size), 100)  # 최대 100건
        keyword_cleaned = keyword.strip() if keyword and keyword.strip() else None
        selected_genres = normalize_search_genre_labels(genres)
        # 선택 장르별 alias 그룹을 유지해야 "몇 개 장르를 만족했는지"를 정렬에 반영할 수 있습니다.
        selected_genre_alias_groups = get_search_genre_alias_groups(selected_genres)
        expanded_genres = expand_search_genre_aliases(selected_genres)
        is_genre_discovery_search = keyword_cleaned is None and bool(selected_genres)
        search_history_keyword = (
            keyword_cleaned if keyword_cleaned is not None else ",".join(selected_genres)
        ) or None
        # 장르 탐색 검색의 평점순 정렬에서만 평점 신뢰도를 위해 최소 평점 참여 인원 수를 적용합니다.
        genre_discovery_vote_count_min = (
            self._settings.GENRE_DISCOVERY_MIN_VOTE_COUNT
            if is_genre_discovery_search and sort_by == "rating"
            else None
        )
        # 관련도순은 프런트에서 기본 검색 결과를 그대로 유지하므로 DB에는 기본 검색 순서를 요청합니다.
        # 최신순은 "검색 시작 시점의 기준 정렬"로 사용되므로 DB에서도 실제 개봉일 정렬을 타야 합니다.
        db_sort_by = "relevance" if sort_by == "relevance" else sort_by
        did_you_mean: str | None = None
        related_queries: list[str] = []
        search_source: str | None = None

        # ─────────────────────────────────────
        # Elasticsearch 우선 검색 (키워드 검색 + 장르 탐색 검색)
        # ─────────────────────────────────────
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
        
        # ─────────────────────────────────────
        # 부수 작업: 검색 이력 + 인기 검색어 갱신
        # ─────────────────────────────────────
        if search_history_keyword:
            # 검색 히스토리는 "검색 실행" 자체만 기록합니다.
            # 무한 스크롤로 이어지는 2페이지 이후 요청은 같은 검색 세션의 연장선이므로 저장하지 않습니다.
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
                    # 검색 이력 저장 실패가 검색 자체를 방해하면 안 됨
                    logger.warning(f"검색 이력 저장 실패 (user_id={user_id}): {e}")

            # Redis 인기 검색어 점수 증가는 텍스트 검색어에만 적용합니다.
            # 장르 탐색 검색은 큐레이션 성격이 강해 실시간 키워드 집계에서 제외합니다.
            if self._redis is not None and keyword_cleaned is not None:
                try:
                    await self._redis.zincrby("trending:keywords", 1, keyword_cleaned)
                except Exception as e:
                    logger.warning(f"Redis 인기 검색어 갱신 실패: {e}")

            # MySQL 인기 검색어 백업도 텍스트 검색어에만 적용합니다.
            if keyword_cleaned is not None:
                try:
                    await self._trending_repo.increment(keyword_cleaned)
                except Exception as e:
                    logger.warning(f"MySQL 인기 검색어 저장 실패: {e}")

        # ─────────────────────────────────────
        # 응답 변환
        # ─────────────────────────────────────
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

    async def get_recent_searches(
        self,
        user_id: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> RecentSearchResponse:
        """
        사용자의 최근 검색어를 반환합니다.

        Args:
            user_id: 사용자 ID
            offset: 중복 제거된 목록 기준 시작 위치
            limit: 페이지당 반환 건수 (최대 10건)

        Returns:
            RecentSearchResponse: 최근 검색어 목록 + 페이지네이션 정보
        """
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
        """
        영화 상세 정보를 반환합니다.

        Args:
            movie_id: 영화 ID

        Returns:
            MovieDetailResponse: 상세 정보

        Raises:
            ValueError: 해당 영화를 찾지 못한 경우
        """
        movie = await self._movie_repo.find_by_id(movie_id)
        if movie is None:
            raise MovieDetailNotFoundError(f"영화 ID '{movie_id}'를 찾을 수 없습니다.")
        return self._to_movie_detail(movie)

    async def delete_recent_keyword(self, user_id: str, keyword: str) -> bool:
        """
        특정 검색어를 최근 검색 이력에서 삭제합니다.

        Args:
            user_id: 사용자 ID
            keyword: 삭제할 키워드

        Returns:
            삭제 성공 여부
        """
        return await self._history_repo.delete_keyword(user_id, keyword)

    async def delete_all_recent(self, user_id: str) -> int:
        """
        사용자의 모든 최근 검색 이력을 삭제합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            삭제된 항목 수
        """
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
            "genres": genres,
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

    def _to_movie_brief(self, movie: Movie) -> MovieBrief:
        """
        Movie 엔티티를 MovieBrief 스키마로 변환합니다.

        포스터 경로를 전체 URL로 조합합니다.
        장르 JSON 문자열을 리스트로 파싱합니다.

        Args:
            movie: Movie 엔티티

        Returns:
            MovieBrief Pydantic 모델
        """
        # 포스터 전체 URL 조합
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        # 장르 JSON 파싱
        genres = movie.get_genres_list()

        return MovieBrief(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=genres,
            release_year=movie.release_year,
            rating=movie.rating,
            # H4NN4N PR #28 장르 탐색 응답이 vote_count 필드를 요구하므로 포함한다.
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

    def _to_movie_detail(self, movie: Movie) -> MovieDetailResponse:
        """
        Movie 엔티티를 MovieDetailResponse로 변환합니다.

        Args:
            movie: Movie 엔티티

        Returns:
            MovieDetailResponse Pydantic 모델
        """
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        backdrop_url = None
        if movie.backdrop_path:
            backdrop_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.backdrop_path}"

        kobis_open_dt = self._normalize_kobis_open_dt(movie.kobis_open_dt)
        release_date = self._normalize_release_date(movie.release_date)
        if release_date is None and kobis_open_dt and len(kobis_open_dt) == 8 and kobis_open_dt.isdigit():
            release_date = f"{kobis_open_dt[:4]}-{kobis_open_dt[4:6]}-{kobis_open_dt[6:8]}"

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

    @staticmethod
    def _normalize_release_date(value: object) -> str | None:
        """release_date 컬럼 값을 YYYY-MM-DD 문자열로 정규화합니다."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                return None
            if len(trimmed) == 8 and trimmed.isdigit():
                return f"{trimmed[:4]}-{trimmed[4:6]}-{trimmed[6:8]}"
            return trimmed
        return str(value)
