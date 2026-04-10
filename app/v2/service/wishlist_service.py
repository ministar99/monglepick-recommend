"""
위시리스트 서비스 (v2 Raw SQL)

Recommend(FastAPI)의 JWT 인증과 aiomysql 커넥션을 사용해
영화 상세 / 마이페이지 위시리스트 기능을 직접 처리한다.
"""

from __future__ import annotations

import logging

import aiomysql

from app.config import get_settings
from app.model.schema import (
    MovieBrief,
    WishlistItem,
    WishlistListResponse,
    WishlistStatusResponse,
    WishlistToggleResponse,
)
from app.v2.model.dto import MovieDTO
from app.v2.repository.wishlist_repository import WishlistRepository

logger = logging.getLogger(__name__)


class WishlistService:
    """위시리스트 비즈니스 로직 서비스."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._settings = get_settings()
        self._repo = WishlistRepository(conn)

    async def get_wishlist(
        self,
        user_id: str,
        page: int = 1,
        size: int = 20,
    ) -> WishlistListResponse:
        """사용자의 위시리스트를 영화 카드 정보와 함께 반환한다."""
        page = max(1, page)
        size = min(max(1, size), 100)
        offset = (page - 1) * size

        rows = await self._repo.list_by_user(user_id, offset, size)
        total = await self._repo.count_by_user(user_id)

        wishlist_items = []
        for row in rows:
            movie = MovieDTO(
                movie_id=row["movie_id"],
                title=row["title"],
                title_en=row.get("title_en"),
                poster_path=row.get("poster_path"),
                release_year=row.get("release_year"),
                rating=row.get("rating"),
                vote_count=row.get("vote_count"),
                genres=row.get("genres"),
                trailer_url=row.get("trailer_url"),
                overview=row.get("overview"),
            )
            wishlist_items.append(
                WishlistItem(
                    wishlist_id=row["wishlist_id"],
                    movie_id=row["wishlist_movie_id"],
                    created_at=row["wishlist_created_at"],
                    movie=self._to_movie_brief(movie),
                )
            )

        return WishlistListResponse(wishlist=wishlist_items, total=total)

    async def get_wishlist_status(self, user_id: str, movie_id: str) -> WishlistStatusResponse:
        """현재 영화의 위시리스트 포함 여부를 반환한다."""
        return WishlistStatusResponse(
            wishlisted=await self._repo.exists(user_id, movie_id),
        )

    async def add_to_wishlist(self, user_id: str, movie_id: str) -> WishlistToggleResponse:
        """위시리스트에 영화를 추가한다."""
        if await self._repo.exists(user_id, movie_id):
            return WishlistToggleResponse(wishlisted=True)

        await self._repo.add(user_id, movie_id)
        logger.info("[v2] 위시리스트 추가 user=%s movie=%s", user_id, movie_id)
        return WishlistToggleResponse(wishlisted=True)

    async def remove_from_wishlist(self, user_id: str, movie_id: str) -> WishlistToggleResponse:
        """위시리스트에서 영화를 제거한다."""
        await self._repo.remove(user_id, movie_id)
        logger.info("[v2] 위시리스트 제거 user=%s movie=%s", user_id, movie_id)
        return WishlistToggleResponse(wishlisted=False)

    def _to_movie_brief(self, movie: MovieDTO) -> MovieBrief:
        """
        위시리스트 영화도 검색 결과 카드와 같은 필드 셋으로 통일한다.

        MovieList 컴포넌트가 바로 렌더링할 수 있게 SearchService와 동일한 모양으로 맞춘다.
        """
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

