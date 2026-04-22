"""
선호 장르 서비스 (v2 Raw SQL)

마이페이지 선호 설정 탭의 선호 장르 조회/저장을 담당합니다.
"""

from __future__ import annotations

import logging

import aiomysql

from app.model.schema import (
    FavoriteGenreItem,
    FavoriteGenreListResponse,
    FavoriteGenreOption,
)
from app.v2.repository.favorite_genre_repository import FavoriteGenreRepository

logger = logging.getLogger(__name__)

EXCLUDED_GENRE_NAMES = ("에로", "동성애", "반공/분단", "계몽", "코메디")


class FavoriteGenreService:
    """선호 장르 비즈니스 로직 서비스."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._repo = FavoriteGenreRepository(conn)

    async def get_favorite_genres(self, user_id: str) -> FavoriteGenreListResponse:
        """
        선호 장르 설정 화면 초기 데이터를 반환합니다.

        - available_genres: 선택 가능한 전체 장르 목록
        - selected_genres: 사용자가 저장한 선호 장르 목록 (priority 순)
        """
        available_rows = await self._repo.list_available_genres(list(EXCLUDED_GENRE_NAMES))
        selected_rows = await self._repo.list_selected_by_user(user_id)
        available_map = {int(row["genre_id"]): row for row in available_rows}

        selected_genres: list[FavoriteGenreItem] = []
        for row in selected_rows:
            genre_id = int(row["genre_id"])
            available_row = available_map.get(genre_id)
            if available_row is None:
                logger.warning(
                    "favorite genre row references hidden/missing genre user=%s genre_id=%s",
                    user_id,
                    genre_id,
                )
                continue

            selected_genres.append(
                FavoriteGenreItem(
                    fav_genre_id=int(row["fav_genre_id"]),
                    genre_id=genre_id,
                    priority=int(row.get("priority") or 0),
                    created_at=row.get("created_at"),
                    genre=self._to_genre_option(available_row),
                )
            )

        return FavoriteGenreListResponse(
            available_genres=[self._to_genre_option(row) for row in available_rows],
            selected_genres=selected_genres,
        )

    async def save_favorite_genres(
        self,
        user_id: str,
        genre_ids: list[int],
    ) -> FavoriteGenreListResponse:
        """
        사용자가 선택한 선호 장르 목록을 저장합니다.

        현재 UI에는 별도 정렬 기능이 없으므로, 선택 순서를 그대로 priority로 사용합니다.
        기존에 저장된 장르 순서는 유지되고, 새로 선택한 장르는 배열 뒤에 붙는 방식입니다.
        """
        normalized_ids = self._normalize_genre_ids(genre_ids)
        await self._validate_genre_ids(normalized_ids)
        await self._repo.replace_all(user_id, normalized_ids)
        logger.info("[v2] 선호 장르 저장 user=%s count=%s", user_id, len(normalized_ids))
        return await self.get_favorite_genres(user_id)

    async def _validate_genre_ids(self, genre_ids: list[int]) -> None:
        """저장 요청의 genre_ids 유효성을 검증합니다."""
        if not genre_ids:
            return

        available_rows = await self._repo.list_available_genres(list(EXCLUDED_GENRE_NAMES))
        allowed_ids = {int(row["genre_id"]) for row in available_rows}
        invalid_ids = [genre_id for genre_id in genre_ids if genre_id not in allowed_ids]
        if invalid_ids:
            raise ValueError(
                "선택할 수 없는 장르가 포함되어 있습니다: "
                + ", ".join(str(genre_id) for genre_id in invalid_ids)
            )

    @staticmethod
    def _normalize_genre_ids(genre_ids: list[int]) -> list[int]:
        """빈 값 제거와 중복 검사를 수행합니다."""
        normalized_ids: list[int] = []
        seen: set[int] = set()

        for raw_genre_id in genre_ids:
            genre_id = int(raw_genre_id)
            if genre_id in seen:
                raise ValueError("같은 장르를 중복해서 저장할 수 없습니다.")
            seen.add(genre_id)
            normalized_ids.append(genre_id)

        return normalized_ids

    @staticmethod
    def _to_genre_option(row: dict) -> FavoriteGenreOption:
        """쿼리 결과를 장르 옵션 스키마로 변환합니다."""
        return FavoriteGenreOption(
            genre_id=int(row["genre_id"]),
            genre_code=row["genre_code"],
            genre_name=row["genre_name"],
            contents_count=int(row.get("contents_count") or 0),
        )
