"""
영화 검색 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 SearchService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import logging
from math import ceil

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    MovieBrief,
    MovieDetailResponse,
    MovieSearchResponse,
    PaginationMeta,
    RecentSearchItem,
    RecentSearchResponse,
)
from app.v2.model.dto import MovieDTO
from app.v2.repository.movie_repository import MovieRepository
from app.v2.repository.search_history_repository import SearchHistoryRepository
from app.v2.repository.trending_repository import TrendingRepository

logger = logging.getLogger(__name__)


class SearchService:
    """영화 검색 비즈니스 로직 서비스 (v2 Raw SQL)"""

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

    async def search_movies(
        self,
        keyword: str | None = None,
        search_type: str = "title",
        genre: str | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        rating_min: float | None = None,
        rating_max: float | None = None,
        sort_by: str = "rating",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
        user_id: str | None = None,
    ) -> MovieSearchResponse:
        """
        영화를 검색하고 필터링된 결과를 반환합니다.

        검색과 동시에 부수 작업 수행:
        - 로그인 사용자: 검색 이력 저장
        - 인기 검색어 점수 증가 (Redis + MySQL)
        """
        # 입력값 정규화
        page = max(1, page)
        size = min(max(1, size), 100)

        # MySQL 검색 실행
        movies, total = await self._movie_repo.search(
            keyword=keyword,
            search_type=search_type,
            genre=genre,
            year_from=year_from,
            year_to=year_to,
            rating_min=rating_min,
            rating_max=rating_max,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            size=size,
        )

        # 부수 작업: 검색 이력 + 인기 검색어 갱신
        if keyword and keyword.strip():
            keyword_cleaned = keyword.strip()

            # 로그인 사용자의 검색 이력 저장
            if user_id:
                try:
                    await self._history_repo.add_search(user_id, keyword_cleaned)
                except Exception as e:
                    logger.warning(f"검색 이력 저장 실패 (user_id={user_id}): {e}")

            # Redis 인기 검색어 점수 증가
            if self._redis is not None:
                try:
                    await self._redis.zincrby("trending:keywords", 1, keyword_cleaned)
                except Exception as e:
                    logger.warning(f"Redis 인기 검색어 갱신 실패: {e}")

            # MySQL 인기 검색어 백업
            try:
                await self._trending_repo.increment(keyword_cleaned)
            except Exception as e:
                logger.warning(f"MySQL 인기 검색어 저장 실패: {e}")

        # 응답 변환
        movie_briefs = [self._to_movie_brief(m) for m in movies]
        total_pages = ceil(total / size) if total > 0 else 0

        return MovieSearchResponse(
            movies=movie_briefs,
            pagination=PaginationMeta(
                page=page,
                size=size,
                total=total,
                total_pages=total_pages,
            ),
        )

    async def get_recent_searches(self, user_id: str) -> RecentSearchResponse:
        """사용자의 최근 검색어를 반환합니다."""
        records = await self._history_repo.get_recent(user_id)
        items = [
            RecentSearchItem(keyword=r.keyword, searched_at=r.searched_at)
            for r in records
        ]
        return RecentSearchResponse(searches=items)

    async def get_movie_detail(self, movie_id: str) -> MovieDetailResponse:
        """영화 상세 정보를 반환합니다."""
        movie = await self._movie_repo.find_by_id(movie_id)
        if movie is None:
            raise ValueError(f"영화 ID '{movie_id}'를 찾을 수 없습니다.")
        return self._to_movie_detail(movie)

    async def delete_recent_keyword(self, user_id: str, keyword: str) -> bool:
        """특정 검색어를 최근 검색 이력에서 삭제합니다."""
        return await self._history_repo.delete_keyword(user_id, keyword)

    async def delete_all_recent(self, user_id: str) -> int:
        """사용자의 모든 최근 검색 이력을 삭제합니다."""
        return await self._history_repo.delete_all(user_id)

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

        release_date = None
        if movie.kobis_open_dt and len(movie.kobis_open_dt) == 8 and movie.kobis_open_dt.isdigit():
            release_date = (
                f"{movie.kobis_open_dt[:4]}-{movie.kobis_open_dt[4:6]}-{movie.kobis_open_dt[6:8]}"
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
            kobis_open_dt=movie.kobis_open_dt,
            awards=movie.awards,
            filming_location=movie.filming_location,
            source=movie.source,
        )
