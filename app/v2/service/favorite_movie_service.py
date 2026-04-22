"""
최애 영화 서비스 (v2 Raw SQL)

마이페이지 선호 설정 탭의 최애 영화 조회/저장/정렬 변경을 담당합니다.
"""

from __future__ import annotations

import logging

import aiomysql

from app.config import get_settings
from app.model.schema import FavoriteMovieItem, FavoriteMovieListResponse
from app.v2.model.dto import MovieDTO
from app.v2.repository.favorite_movie_repository import FavoriteMovieRepository
from app.v2.repository.movie_repository import MovieRepository

logger = logging.getLogger(__name__)

MAX_FAVORITE_MOVIES = 9


class FavoriteMovieService:
    """최애 영화 비즈니스 로직 서비스."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._settings = get_settings()
        self._repo = FavoriteMovieRepository(conn)
        self._movie_repo = MovieRepository(conn)

    async def get_favorite_movies(self, user_id: str) -> FavoriteMovieListResponse:
        """사용자의 최애 영화 목록을 반환합니다."""
        rows = await self._repo.list_by_user(user_id)
        movie_ids = [row["movie_id"] for row in rows]
        movies = await self._load_movies_in_order(movie_ids)
        movie_map = {movie.movie_id: movie for movie in movies}

        favorite_movies = []
        for row in rows:
            movie = movie_map.get(row["movie_id"])
            if movie is None:
                logger.warning(
                    "favorite movie row references missing movie user=%s movie_id=%s",
                    user_id,
                    row["movie_id"],
                )
                continue
            favorite_movies.append(
                FavoriteMovieItem(
                    fav_movie_id=row["fav_movie_id"],
                    movie_id=row["movie_id"],
                    priority=row.get("priority") or 0,
                    created_at=row.get("created_at"),
                    movie=self._to_movie_brief(movie),
                )
            )

        return FavoriteMovieListResponse(
            favorite_movies=favorite_movies,
            total=len(favorite_movies),
            max_count=MAX_FAVORITE_MOVIES,
        )

    async def save_favorite_movies(
        self,
        user_id: str,
        movie_ids: list[str],
    ) -> FavoriteMovieListResponse:
        """모달에서 선택한 최애 영화 목록을 저장합니다."""
        normalized_ids = self._normalize_movie_ids(movie_ids)
        await self._validate_movie_ids(normalized_ids)
        await self._repo.replace_all(user_id, normalized_ids)
        logger.info("[v2] 최애 영화 저장 user=%s count=%s", user_id, len(normalized_ids))
        return await self.get_favorite_movies(user_id)

    async def reorder_favorite_movies(
        self,
        user_id: str,
        movie_ids: list[str],
    ) -> FavoriteMovieListResponse:
        """기존 최애 영화의 priority 순서를 저장합니다."""
        normalized_ids = self._normalize_movie_ids(movie_ids)
        current_rows = await self._repo.list_by_user(user_id)
        current_ids = [row["movie_id"] for row in current_rows]

        if len(normalized_ids) != len(current_ids) or set(normalized_ids) != set(current_ids):
            raise ValueError("현재 저장된 최애 영화와 동일한 목록으로만 순서를 변경할 수 있습니다.")

        await self._repo.replace_all(user_id, normalized_ids)
        logger.info("[v2] 최애 영화 순서 저장 user=%s count=%s", user_id, len(normalized_ids))
        return await self.get_favorite_movies(user_id)

    async def _validate_movie_ids(self, movie_ids: list[str]) -> None:
        """저장 요청의 movie_ids 유효성을 검증합니다."""
        if len(movie_ids) > MAX_FAVORITE_MOVIES:
            raise ValueError(f"최애 영화는 최대 {MAX_FAVORITE_MOVIES}편까지 저장할 수 있습니다.")

        if not movie_ids:
            return

        movies = await self._load_movies_in_order(movie_ids)
        if len(movies) != len(movie_ids):
            found_ids = {movie.movie_id for movie in movies}
            missing_ids = [movie_id for movie_id in movie_ids if movie_id not in found_ids]
            raise ValueError(f"존재하지 않는 영화가 포함되어 있습니다: {', '.join(missing_ids)}")

    async def _load_movies_in_order(self, movie_ids: list[str]) -> list[MovieDTO]:
        """영화 ID 목록을 입력 순서 그대로 MovieDTO 리스트로 반환합니다."""
        if not movie_ids:
            return []

        movies = await self._movie_repo.find_by_ids(movie_ids)
        movie_map = {movie.movie_id: movie for movie in movies}
        return [movie_map[movie_id] for movie_id in movie_ids if movie_id in movie_map]

    @staticmethod
    def _normalize_movie_ids(movie_ids: list[str]) -> list[str]:
        """빈 값 제거와 중복 검사를 수행합니다."""
        normalized_ids: list[str] = []
        seen: set[str] = set()

        for raw_movie_id in movie_ids:
            movie_id = str(raw_movie_id).strip()
            if not movie_id:
                continue
            if movie_id in seen:
                raise ValueError("같은 영화를 중복해서 저장할 수 없습니다.")
            seen.add(movie_id)
            normalized_ids.append(movie_id)

        return normalized_ids

    def _to_movie_brief(self, movie: MovieDTO):
        """MovieDTO를 MovieBrief 스키마로 변환합니다."""
        from app.model.schema import MovieBrief

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
