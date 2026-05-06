"""
리뷰 리포지토리 (v2 Raw SQL)

reviews / review_likes 테이블을 aiomysql로 직접 읽고 쓴다.
영화 상세의 리뷰 조회·작성·수정·삭제·좋아요 토글을 recommend 서비스로 이관하기 위한
최소 SQL 집합을 제공한다.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiomysql


class ReviewRepository:
    """리뷰 MySQL 리포지토리."""

    # SQL Injection 방지: _get_columns() 에 전달 가능한 테이블명 화이트리스트.
    # 이 집합에 없는 테이블명이 인자로 들어오면 즉시 ValueError 를 발생시킨다.
    _ALLOWED_TABLES: frozenset[str] = frozenset({"reviews", "review_likes"})

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        self._columns_cache: dict[str, set[str]] = {}

    async def _get_columns(self, table_name: str) -> set[str]:
        """
        실제 테이블 컬럼 목록을 캐시한다.

        로컬 DB는 구버전(init.sql) 스키마를 쓰고 있을 수 있어
        reviews.id/content 와 최신 review_id/contents 를 모두 지원해야 한다.

        SQL Injection 방지를 위해 table_name 을 _ALLOWED_TABLES 로 검증한다.
        허용 목록에 없는 테이블명이 전달되면 ValueError 를 발생시킨다.
        """
        # 허용 목록 검증 — SHOW COLUMNS 는 파라미터 바인딩을 지원하지 않아
        # f-string 으로 테이블명을 삽입하기 전 반드시 allowlist 를 통과해야 한다.
        if table_name not in self._ALLOWED_TABLES:
            raise ValueError(f"허용되지 않은 테이블: {table_name}")

        if table_name in self._columns_cache:
            return self._columns_cache[table_name]

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SHOW COLUMNS FROM {table_name}")
            rows = await cur.fetchall()

        columns = {row["Field"] for row in rows or []}
        self._columns_cache[table_name] = columns
        return columns

    async def list_by_movie(
        self,
        movie_id: str,
        offset: int,
        limit: int,
        sort: str = "latest",
        current_user_id: str | None = None,
    ) -> list[dict]:
        """
        특정 영화의 리뷰 목록을 조회한다.

        현재 화면에서 쓰는 정렬은 최신순이 기본이지만,
        클라이언트 호환을 위해 평점순 옵션도 함께 지원한다.
        """
        if sort == "rating_high":
            order_by = "r.rating DESC, r.created_at DESC"
        elif sort == "rating_low":
            order_by = "r.rating ASC, r.created_at DESC"
        else:
            order_by = "r.created_at DESC"

        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"
        content_column = "contents" if "contents" in review_columns else "content"
        where_clauses = ["r.movie_id = %s"]

        # 최신 운영 스키마의 soft-delete / blind 컬럼이 있을 때만 필터를 건다.
        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(r.is_deleted, 0) = 0")
        if "is_blinded" in review_columns:
            where_clauses.append("COALESCE(r.is_blinded, 0) = 0")

        review_source_select = (
            "r.review_source AS review_source"
            if "review_source" in review_columns
            else "NULL AS review_source"
        )
        review_category_select = (
            "r.review_category_code AS review_category_code"
            if "review_category_code" in review_columns
            else "NULL AS review_category_code"
        )
        like_status_select = "0 AS liked"
        params: list[object] = [movie_id, limit, offset]
        if current_user_id is not None:
            like_status_select = (
                "EXISTS(SELECT 1 FROM review_likes rl2 "
                f"WHERE rl2.review_id = r.{review_id_column} AND rl2.user_id = %s) AS liked"
            )
            params = [current_user_id, movie_id, limit, offset]

        sql = (
            "SELECT "
            f"  r.{review_id_column} AS id, "
            "  r.user_id, "
            "  r.movie_id, "
            "  r.rating, "
            f"  r.{content_column} AS content, "
            "  COALESCE(u.nickname, '익명') AS author_nickname, "
            f"  {('r.is_spoiler AS is_spoiler' if 'is_spoiler' in review_columns else 'r.spoiler AS is_spoiler' if 'spoiler' in review_columns else '0 AS is_spoiler')}, "
            f"  {review_source_select}, "
            f"  {review_category_select}, "
            "  r.created_at, "
            f"  (SELECT COUNT(*) FROM review_likes rl WHERE rl.review_id = r.{review_id_column}) AS like_count, "
            f"  {like_status_select} "
            "FROM reviews r "
            "LEFT JOIN users u ON u.user_id = r.user_id "
            f"WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY {order_by} "
            "LIMIT %s OFFSET %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, tuple(params))
            return list(await cur.fetchall() or [])

    async def count_by_movie(self, movie_id: str) -> int:
        """특정 영화의 전체 활성 리뷰 수를 반환한다."""
        review_columns = await self._get_columns("reviews")
        where_clauses = ["movie_id = %s"]
        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(is_deleted, 0) = 0")
        if "is_blinded" in review_columns:
            where_clauses.append("COALESCE(is_blinded, 0) = 0")

        sql = (
            "SELECT COUNT(*) AS cnt "
            "FROM reviews "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (movie_id,))
            row = await cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0

    async def list_by_user(
        self,
        user_id: str,
        offset: int,
        limit: int,
        current_user_id: str | None = None,
    ) -> list[dict]:
        """
        특정 사용자가 작성한 리뷰 목록을 최신순으로 조회한다.

        마이페이지 탭은 영화 상세의 리뷰 카드 외형을 그대로 재사용하므로
        작성자 닉네임, 좋아요 수, 영화 제목까지 한 번에 내려준다.
        """
        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"
        content_column = "contents" if "contents" in review_columns else "content"
        where_clauses = ["r.user_id = %s"]

        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(r.is_deleted, 0) = 0")
        if "is_blinded" in review_columns:
            where_clauses.append("COALESCE(r.is_blinded, 0) = 0")

        review_source_select = (
            "r.review_source AS review_source"
            if "review_source" in review_columns
            else "NULL AS review_source"
        )
        review_category_select = (
            "r.review_category_code AS review_category_code"
            if "review_category_code" in review_columns
            else "NULL AS review_category_code"
        )
        like_status_select = "0 AS liked"
        params: list[object] = [user_id, limit, offset]
        if current_user_id is not None:
            like_status_select = (
                "EXISTS(SELECT 1 FROM review_likes rl2 "
                f"WHERE rl2.review_id = r.{review_id_column} AND rl2.user_id = %s) AS liked"
            )
            params = [current_user_id, user_id, limit, offset]

        sql = (
            "SELECT "
            f"  r.{review_id_column} AS id, "
            "  r.user_id, "
            "  r.movie_id, "
            "  m.title AS movie_title, "
            "  m.poster_path AS poster_path, "
            "  r.rating, "
            f"  r.{content_column} AS content, "
            "  COALESCE(u.nickname, '익명') AS author_nickname, "
            f"  {('r.is_spoiler AS is_spoiler' if 'is_spoiler' in review_columns else 'r.spoiler AS is_spoiler' if 'spoiler' in review_columns else '0 AS is_spoiler')}, "
            f"  {review_source_select}, "
            f"  {review_category_select}, "
            "  r.created_at, "
            f"  (SELECT COUNT(*) FROM review_likes rl WHERE rl.review_id = r.{review_id_column}) AS like_count, "
            f"  {like_status_select} "
            "FROM reviews r "
            "LEFT JOIN users u ON u.user_id = r.user_id "
            "LEFT JOIN movies m ON m.movie_id = r.movie_id "
            f"WHERE {' AND '.join(where_clauses)} "
            "ORDER BY r.created_at DESC "
            "LIMIT %s OFFSET %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, tuple(params))
            return list(await cur.fetchall() or [])

    async def count_by_user(self, user_id: str) -> int:
        """특정 사용자가 작성한 전체 활성 리뷰 수를 반환한다."""
        review_columns = await self._get_columns("reviews")
        where_clauses = ["user_id = %s"]
        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(is_deleted, 0) = 0")
        if "is_blinded" in review_columns:
            where_clauses.append("COALESCE(is_blinded, 0) = 0")

        sql = (
            "SELECT COUNT(*) AS cnt "
            "FROM reviews "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            row = await cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0

    async def get_user_average_rating(
        self,
        user_id: str,
        *,
        exclude_review_id: int | None = None,
    ) -> float | None:
        """특정 사용자의 활성 리뷰 평균 평점을 반환한다."""
        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"
        where_clauses = ["user_id = %s"]
        params: list[object] = [user_id]

        if exclude_review_id is not None:
            where_clauses.append(f"{review_id_column} <> %s")
            params.append(exclude_review_id)
        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(is_deleted, 0) = 0")
        if "is_blinded" in review_columns:
            where_clauses.append("COALESCE(is_blinded, 0) = 0")

        sql = (
            "SELECT AVG(rating) AS avg_rating "
            "FROM reviews "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, tuple(params))
            row = await cur.fetchone()

        if not row or row.get("avg_rating") is None:
            return None
        return float(row["avg_rating"])

    async def exists_by_user_movie(self, user_id: str, movie_id: str) -> bool:
        """동일 사용자의 동일 영화 활성 리뷰 존재 여부를 확인한다."""
        review_columns = await self._get_columns("reviews")
        where_clauses = ["user_id = %s", "movie_id = %s"]

        # 소프트 삭제된 리뷰는 재작성 가능해야 하므로 중복 검사에서 제외한다.
        if "is_deleted" in review_columns:
            where_clauses.append("COALESCE(is_deleted, 0) = 0")

        sql = (
            "SELECT 1 "
            "FROM reviews "
            f"WHERE {' AND '.join(where_clauses)} "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, movie_id))
            row = await cur.fetchone()
        return row is not None

    async def find_by_id(self, review_id: int) -> dict | None:
        """리뷰 ID로 단건 조회한다."""
        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"
        content_column = "contents" if "contents" in review_columns else "content"
        review_source_select = (
            "r.review_source AS review_source"
            if "review_source" in review_columns
            else "NULL AS review_source"
        )
        review_category_select = (
            "r.review_category_code AS review_category_code"
            if "review_category_code" in review_columns
            else "NULL AS review_category_code"
        )

        sql = (
            "SELECT "
            f"  r.{review_id_column} AS id, "
            "  r.user_id, "
            "  r.movie_id, "
            "  m.title AS movie_title, "
            "  m.poster_path AS poster_path, "
            "  r.rating, "
            f"  r.{content_column} AS content, "
            "  COALESCE(u.nickname, '익명') AS author_nickname, "
            f"  {('r.is_spoiler AS is_spoiler' if 'is_spoiler' in review_columns else 'r.spoiler AS is_spoiler' if 'spoiler' in review_columns else '0 AS is_spoiler')}, "
            f"  {review_source_select}, "
            f"  {review_category_select}, "
            "  r.created_at, "
            f"  (SELECT COUNT(*) FROM review_likes rl WHERE rl.review_id = r.{review_id_column}) AS like_count "
            "FROM reviews r "
            "LEFT JOIN users u ON u.user_id = r.user_id "
            "LEFT JOIN movies m ON m.movie_id = r.movie_id "
            f"WHERE r.{review_id_column} = %s "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (review_id,))
            return await cur.fetchone()

    async def create(
        self,
        *,
        user_id: str,
        movie_id: str,
        rating: float,
        content: str | None,
        is_spoiler: bool,
        review_source: str | None,
        review_category_code: str | None,
    ) -> dict:
        """리뷰를 생성하고 생성된 행을 반환한다."""
        now = datetime.now(timezone.utc)
        review_columns = await self._get_columns("reviews")
        content_column = "contents" if "contents" in review_columns else "content"
        insert_columns = ["user_id", "movie_id", "rating", content_column]
        values: list[object] = [user_id, movie_id, rating, content]

        if "is_deleted" in review_columns:
            insert_columns.append("is_deleted")
            values.append(0)
        if "is_blinded" in review_columns:
            insert_columns.append("is_blinded")
            values.append(0)
        if "is_spoiler" in review_columns:
            insert_columns.append("is_spoiler")
            values.append(1 if is_spoiler else 0)
        elif "spoiler" in review_columns:
            insert_columns.append("spoiler")
            values.append(1 if is_spoiler else 0)
        if "like_count" in review_columns:
            insert_columns.append("like_count")
            values.append(0)
        if "review_source" in review_columns:
            insert_columns.append("review_source")
            values.append(review_source)
        if "review_category_code" in review_columns:
            insert_columns.append("review_category_code")
            values.append(review_category_code)
        if "created_at" in review_columns:
            insert_columns.append("created_at")
            values.append(now)
        if "updated_at" in review_columns:
            insert_columns.append("updated_at")
            values.append(now)
        if "created_by" in review_columns:
            insert_columns.append("created_by")
            values.append(user_id)
        if "updated_by" in review_columns:
            insert_columns.append("updated_by")
            values.append(user_id)

        placeholders = ", ".join(["%s"] * len(insert_columns))
        sql = (
            "INSERT INTO reviews "
            f"({', '.join(insert_columns)}) "
            f"VALUES ({placeholders})"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, tuple(values))
            review_id = cur.lastrowid
        return await self.find_by_id(int(review_id))

    async def update(
        self,
        *,
        review_id: int,
        rating: float,
        content: str | None,
        is_spoiler: bool,
        user_id: str,
    ) -> dict | None:
        """리뷰 내용과 평점을 수정하고 수정된 행을 반환한다."""
        now = datetime.now(timezone.utc)
        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"
        content_column = "contents" if "contents" in review_columns else "content"
        update_sets = [
            "rating = %s",
            f"{content_column} = %s",
        ]
        values: list[object] = [rating, content]

        # 스포일러 토글은 구버전(spoiler) / 최신(is_spoiler) 컬럼을 모두 지원한다.
        if "is_spoiler" in review_columns:
            update_sets.append("is_spoiler = %s")
            values.append(1 if is_spoiler else 0)
        elif "spoiler" in review_columns:
            update_sets.append("spoiler = %s")
            values.append(1 if is_spoiler else 0)

        if "updated_at" in review_columns:
            update_sets.append("updated_at = %s")
            values.append(now)
        if "updated_by" in review_columns:
            update_sets.append("updated_by = %s")
            values.append(user_id)

        values.append(review_id)
        sql = (
            "UPDATE reviews "
            f"SET {', '.join(update_sets)} "
            f"WHERE {review_id_column} = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, tuple(values))
        return await self.find_by_id(review_id)

    async def delete(self, review_id: int) -> bool:
        """
        리뷰를 삭제한다.

        is_deleted 컬럼이 존재하는 경우: 소프트 삭제 (UPDATE SET is_deleted = 1).
        is_deleted 컬럼이 없는 경우: 하드 삭제 (DELETE FROM reviews).
          - 하드 삭제 시 FK 제약 또는 고아 행 방지를 위해
            review_likes 행을 먼저 삭제한 뒤 reviews 행을 삭제한다.
        """
        review_columns = await self._get_columns("reviews")
        review_id_column = "review_id" if "review_id" in review_columns else "id"

        if "is_deleted" in review_columns:
            # 소프트 삭제: 행을 유지한 채 is_deleted 플래그만 세운다.
            # list/count 쿼리에서 COALESCE(is_deleted, 0) = 0 필터로 자동 제외된다.
            sql = (
                "UPDATE reviews "
                f"SET is_deleted = 1 "
                f"WHERE {review_id_column} = %s"
            )
            async with self._conn.cursor() as cur:
                await cur.execute(sql, (review_id,))
                return cur.rowcount > 0
        else:
            # 하드 삭제: review_likes 를 먼저 제거하여 고아 행을 방지한다.
            async with self._conn.cursor() as cur:
                # 1단계: 해당 리뷰의 좋아요 행 전체 삭제
                await cur.execute(
                    "DELETE FROM review_likes WHERE review_id = %s",
                    (review_id,),
                )
                # 2단계: 리뷰 본체 삭제
                await cur.execute(
                    f"DELETE FROM reviews WHERE {review_id_column} = %s",
                    (review_id,),
                )
                return cur.rowcount > 0

    async def has_review_like(self, review_id: int, user_id: str) -> bool:
        """사용자의 리뷰 좋아요 여부를 조회한다."""
        sql = (
            "SELECT 1 "
            "FROM review_likes "
            "WHERE review_id = %s AND user_id = %s "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (review_id, user_id))
            row = await cur.fetchone()
        return row is not None

    async def insert_review_like(self, review_id: int, user_id: str) -> None:
        """
        리뷰 좋아요를 추가한다.

        review_likes 테이블의 실제 컬럼을 런타임에 확인하여
        created_at / updated_at / created_by / updated_by 가 존재하는 경우에만
        INSERT 컬럼에 포함한다. create() 의 동적 컬럼 패턴을 동일하게 적용한다.
        """
        now = datetime.now(timezone.utc)
        # review_likes 테이블의 실제 컬럼 목록 확인 (캐시 활용)
        like_columns = await self._get_columns("review_likes")

        # 필수 컬럼: review_id, user_id
        insert_columns: list[str] = ["review_id", "user_id"]
        values: list[object] = [review_id, user_id]

        # BaseAuditEntity 컬럼이 존재하는 경우에만 포함 (구버전 스키마 호환)
        if "created_at" in like_columns:
            insert_columns.append("created_at")
            values.append(now)
        if "updated_at" in like_columns:
            insert_columns.append("updated_at")
            values.append(now)
        if "created_by" in like_columns:
            insert_columns.append("created_by")
            values.append(user_id)
        if "updated_by" in like_columns:
            insert_columns.append("updated_by")
            values.append(user_id)

        placeholders = ", ".join(["%s"] * len(insert_columns))
        sql = (
            "INSERT INTO review_likes "
            f"({', '.join(insert_columns)}) "
            f"VALUES ({placeholders})"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, tuple(values))

    async def delete_review_like(self, review_id: int, user_id: str) -> bool:
        """리뷰 좋아요를 취소한다."""
        sql = (
            "DELETE FROM review_likes "
            "WHERE review_id = %s AND user_id = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (review_id, user_id))
            return cur.rowcount > 0

    async def count_review_likes(self, review_id: int) -> int:
        """특정 리뷰의 전체 좋아요 수를 반환한다."""
        sql = "SELECT COUNT(*) AS cnt FROM review_likes WHERE review_id = %s"
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (review_id,))
            row = await cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0
