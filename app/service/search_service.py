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
from math import ceil

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.entity import Movie
from app.model.schema import (
    MovieBrief,
    MovieSearchResponse,
    PaginationMeta,
    RecentSearchItem,
    RecentSearchResponse,
)
from app.repository.movie_repository import MovieRepository
from app.repository.search_history_repository import SearchHistoryRepository
from app.repository.trending_repository import TrendingRepository

logger = logging.getLogger(__name__)


class SearchService:
    """영화 검색 비즈니스 로직 서비스"""

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
        self._history_repo = SearchHistoryRepository(session)
        self._trending_repo = TrendingRepository(session)

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

        검색과 동시에 다음 부수 작업을 수행합니다:
        - 로그인 사용자: 검색 이력 저장 (최근 검색어)
        - 인기 검색어 점수 증가 (Redis Sorted Set)
        - 인기 검색어 MySQL 백업 (TrendingKeyword)

        Args:
            keyword: 검색 키워드
            search_type: 검색 대상 ("title", "director", "actor", "all")
            genre: 장르 필터
            year_from: 시작 연도
            year_to: 끝 연도
            rating_min: 최소 평점
            rating_max: 최대 평점
            sort_by: 정렬 기준 ("rating", "release_date", "title")
            sort_order: 정렬 방향 ("asc", "desc")
            page: 페이지 번호 (1부터)
            size: 페이지 크기
            user_id: 로그인 사용자 ID (None이면 비로그인)

        Returns:
            MovieSearchResponse: 검색 결과 + 페이지네이션
        """
        # 입력값 정규화
        page = max(1, page)
        size = min(max(1, size), 100)  # 최대 100건

        # ─────────────────────────────────────
        # MySQL 검색 실행
        # ─────────────────────────────────────
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

        # ─────────────────────────────────────
        # 부수 작업: 검색 이력 + 인기 검색어 갱신
        # ─────────────────────────────────────
        if keyword and keyword.strip():
            keyword_cleaned = keyword.strip()

            # 로그인 사용자의 검색 이력 저장
            if user_id:
                try:
                    await self._history_repo.add_search(user_id, keyword_cleaned)
                except Exception as e:
                    # 검색 이력 저장 실패가 검색 자체를 방해하면 안 됨
                    logger.warning(f"검색 이력 저장 실패 (user_id={user_id}): {e}")

            # Redis 인기 검색어 점수 증가 (Sorted Set)
            try:
                await self._redis.zincrby("trending:keywords", 1, keyword_cleaned)
            except Exception as e:
                logger.warning(f"Redis 인기 검색어 갱신 실패: {e}")

            # MySQL 인기 검색어 백업 (비동기, 실패해도 무시)
            try:
                await self._trending_repo.increment(keyword_cleaned)
            except Exception as e:
                logger.warning(f"MySQL 인기 검색어 저장 실패: {e}")

        # ─────────────────────────────────────
        # 응답 변환
        # ─────────────────────────────────────
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
        """
        사용자의 최근 검색어를 반환합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            RecentSearchResponse: 최근 검색어 목록 (최대 20건)
        """
        records = await self._history_repo.get_recent(user_id)
        items = [
            RecentSearchItem(keyword=r.keyword, searched_at=r.searched_at)
            for r in records
        ]
        return RecentSearchResponse(searches=items)

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
            poster_url=poster_url,
            overview=movie.overview,
        )
