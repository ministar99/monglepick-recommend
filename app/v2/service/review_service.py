"""
리뷰 서비스 (v2 Raw SQL)

영화 상세 화면에서 사용하는 리뷰 조회/작성/수정/삭제/좋아요 토글을
recommend 서비스로 이관하기 위한 비즈니스 로직을 담는다.
"""

from __future__ import annotations

import logging

import aiomysql

from app.config import get_settings
from app.model.schema import (
    LikeResponse,
    PaginationMeta,
    ReviewAuthor,
    ReviewCreateRequest,
    ReviewItem,
    ReviewListResponse,
    ReviewUpdateRequest,
    UserReviewListResponse,
)
from app.v2.repository.review_repository import ReviewRepository

logger = logging.getLogger(__name__)


class DuplicateReviewError(Exception):
    """
    동일 사용자가 동일 영화에 이미 리뷰를 작성한 경우 발생시키는 예외.

    Backend(`monglepick-backend`) `ReviewService.create()` 가
    `ErrorCode.DUPLICATE_REVIEW` → HTTP 409 로 반환하는 정책과 일관성을 맞추기 위해,
    Recommend v2 도 동일 케이스에서 409 Conflict 를 내보낸다.

    API 레이어(`app.v2.api.review.create_review`) 에서 HTTPException(409) 로 매핑된다.
    """

    pass


class ReviewService:
    """리뷰 비즈니스 로직 서비스."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._repo = ReviewRepository(conn)
        self._settings = get_settings()

    async def get_reviews(
        self,
        movie_id: str,
        page: int = 1,
        size: int = 20,
        sort: str = "latest",
        user_id: str | None = None,
    ) -> ReviewListResponse:
        """특정 영화의 리뷰 목록을 반환한다."""
        page = max(1, page)
        size = min(max(1, size), 100)
        offset = (page - 1) * size

        rows = await self._repo.list_by_movie(movie_id, offset, size, sort)
        total = await self._repo.count_by_movie(movie_id)
        reviews = [self._to_review_item(row, user_id) for row in rows]
        return ReviewListResponse(reviews=reviews, total=total)

    async def get_user_reviews(
        self,
        user_id: str,
        page: int = 1,
        size: int = 20,
    ) -> UserReviewListResponse:
        """마이페이지에서 사용할 내 리뷰 목록을 최신순으로 반환한다."""
        page = max(1, page)
        size = min(max(1, size), 100)
        offset = (page - 1) * size

        rows = await self._repo.list_by_user(user_id, offset, size)
        total = await self._repo.count_by_user(user_id)
        total_pages = (total + size - 1) // size if total > 0 else 0

        return UserReviewListResponse(
            reviews=[self._to_review_item(row, user_id) for row in rows],
            pagination=PaginationMeta(
                page=page,
                size=size,
                total=total,
                total_pages=total_pages,
            ),
        )

    async def create_review(
        self,
        movie_id: str,
        payload: ReviewCreateRequest,
        user_id: str,
    ) -> ReviewItem:
        """영화 리뷰를 작성한다.

        같은 사용자가 같은 영화에 대해서는 1개의 리뷰만 허용한다.
        ("봤다 = 리뷰" 단일 진실 원본 원칙, CLAUDE.md 참조)
        이미 리뷰를 작성했다면 DuplicateReviewError 를 던지고
        API 레이어에서 HTTP 409 로 변환된다.
        """
        if payload.movie_id and payload.movie_id != movie_id:
            raise ValueError("요청 본문의 movie_id와 경로의 movie_id가 일치하지 않습니다.")

        # 중복 리뷰 차단 — Backend ReviewService 와 정책 일치 (1 유저 1 영화 1 리뷰).
        # race condition 으로 INSERT 직전에 중복이 발생할 여지는 남지만,
        # reviews 테이블에 (user_id, movie_id) UNIQUE 제약이 설정되어 있으면 DB 레이어에서 최종 차단된다.
        if await self._repo.exists_by_user_movie(user_id, movie_id):
            raise DuplicateReviewError("이미 이 영화에 리뷰를 작성하셨습니다.")

        row = await self._repo.create(
            user_id=user_id,
            movie_id=movie_id,
            rating=payload.rating,
            content=payload.content,
            is_spoiler=payload.is_spoiler,
            review_source=payload.review_source,
            review_category_code=payload.review_category_code,
        )
        logger.info("[v2] 리뷰 작성 user=%s movie=%s", user_id, movie_id)
        return self._to_review_item(row, user_id)

    async def update_review(
        self,
        movie_id: str,
        review_id: int,
        payload: ReviewUpdateRequest,
        user_id: str,
    ) -> ReviewItem:
        """작성자 본인의 리뷰만 수정한다."""
        existing = await self._repo.find_by_id(review_id)
        if existing is None or existing["movie_id"] != movie_id:
            raise LookupError("리뷰를 찾을 수 없습니다.")
        if existing["user_id"] != user_id:
            raise PermissionError("본인 리뷰만 수정할 수 있습니다.")

        row = await self._repo.update(
            review_id=review_id,
            rating=payload.rating,
            content=payload.content,
            is_spoiler=payload.is_spoiler,
            user_id=user_id,
        )
        logger.info("[v2] 리뷰 수정 user=%s review=%s", user_id, review_id)
        return self._to_review_item(row, user_id)

    async def delete_review(
        self,
        movie_id: str,
        review_id: int,
        user_id: str,
    ) -> None:
        """작성자 본인의 리뷰만 삭제한다."""
        existing = await self._repo.find_by_id(review_id)
        if existing is None or existing["movie_id"] != movie_id:
            raise LookupError("리뷰를 찾을 수 없습니다.")
        if existing["user_id"] != user_id:
            raise PermissionError("본인 리뷰만 삭제할 수 있습니다.")

        await self._repo.delete(review_id)
        logger.info("[v2] 리뷰 삭제 user=%s review=%s", user_id, review_id)

    async def toggle_review_like(self, movie_id: str, review_id: int, user_id: str) -> LikeResponse:
        """리뷰 좋아요를 토글한다."""
        existing = await self._repo.find_by_id(review_id)
        if existing is None or existing["movie_id"] != movie_id:
            raise LookupError("리뷰를 찾을 수 없습니다.")

        liked = not await self._repo.has_review_like(review_id, user_id)
        if liked:
            await self._repo.insert_review_like(review_id, user_id)
        else:
            await self._repo.delete_review_like(review_id, user_id)

        like_count = await self._repo.count_review_likes(review_id)
        return LikeResponse(liked=liked, like_count=like_count)

    def _to_review_item(self, row: dict, current_user_id: str | None = None) -> ReviewItem:
        """DictCursor 결과를 ReviewItem 응답 스키마로 변환한다."""
        # 마이페이지 내 리뷰 목록은 카드 안에 작은 포스터 썸네일을 함께 노출한다.
        poster_url = None
        if row.get("poster_path"):
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{row['poster_path']}"

        return ReviewItem(
            id=int(row["id"]),
            movie_id=row["movie_id"],
            movie_title=row.get("movie_title"),
            poster_url=poster_url,
            rating=float(row["rating"]),
            content=row.get("content"),
            author=ReviewAuthor(nickname=row.get("author_nickname") or "익명"),
            is_spoiler=self._to_bool(row.get("is_spoiler")),
            is_mine=current_user_id is not None and row.get("user_id") == current_user_id,
            review_source=row.get("review_source"),
            review_category_code=row.get("review_category_code"),
            created_at=row["created_at"],
            like_count=int(row.get("like_count") or 0),
        )

    @staticmethod
    def _to_bool(value: object) -> bool:
        """
        MySQL BIT/TINYINT 값을 파이썬 bool로 정규화한다.

        aiomysql 환경에 따라 0/1, bool, bytes(b"\\x00"/b"\\x01")가 섞여 들어올 수 있다.
        """
        if isinstance(value, (bytes, bytearray)):
            return value not in (b"\x00", b"")
        return bool(value)
