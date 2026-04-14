"""
Movie Match — "둘 다 좋아한 사용자가 좋아한 영화" CF 리포지토리 (v2 Raw SQL).

## 배경
Movie Match Agent 의 "둘이 영화 고르기" 추천 품질을 개선하기 위해, 교집합 특성 기반
하이브리드 검색(Qdrant/ES/Neo4j)에 더하여 협업 필터링(Co-watched CF) 후보를 주입한다.

## 핵심 아이디어
두 영화를 **모두 높게 평가한 사용자** 가 본(그리고 높게 평가한) **다른 영화** 를 찾는다.
이는 "두 영화를 동시에 좋아하는 사용자가 좋아할 영화" 라는 본질적 목표와 정합한다.

## 데이터 소스
- `reviews` 테이블 (단일 진실 원본 — CLAUDE.md "봤다 = 리뷰" 원칙)
  - rating >= 3.5 (5점 만점 기준 70% 이상) 인 리뷰를 "좋아함" 신호로 간주
  - soft-delete/blind 컬럼이 있으면 활성 리뷰만 포함

## SQL 개요
```sql
-- 두 영화 모두 rating >= threshold 인 공통 사용자 집합
WITH co_users AS (
    SELECT user_id FROM reviews
    WHERE movie_id = :mid1 AND rating >= :th AND is_deleted = 0
    INTERSECT
    SELECT user_id FROM reviews
    WHERE movie_id = :mid2 AND rating >= :th AND is_deleted = 0
)
-- 공통 사용자들이 좋아한 다른 영화 집계
SELECT movie_id, COUNT(DISTINCT user_id) AS co_user_count, AVG(rating) AS avg_rating
FROM reviews
WHERE user_id IN (SELECT user_id FROM co_users)
  AND movie_id NOT IN (:mid1, :mid2)
  AND rating >= :th
  AND is_deleted = 0
GROUP BY movie_id
ORDER BY co_user_count DESC, avg_rating DESC
LIMIT :top_k
```
MySQL 8.0 는 INTERSECT 미지원이므로 실제 구현은 INNER JOIN / IN (SELECT) 형태로 변환.

## 성능 고려
- `reviews(movie_id, user_id, rating)` 인덱스 활용 가정.
- 둘 다 평가한 사용자 수가 클 경우 쿼리 시간이 길어질 수 있으므로
  서비스 레이어에서 Redis 캐싱 (TTL 5분) 으로 보완.
"""

from __future__ import annotations

import aiomysql


class MatchCowatchRepository:
    """Movie Match CF 용 MySQL 리포지토리."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn
        # reviews 테이블의 실제 컬럼을 캐시하여 구/신 스키마 호환성 보장
        # (review_repository.py 와 동일한 패턴)
        self._review_columns: set[str] | None = None

    async def _get_review_columns(self) -> set[str]:
        """reviews 테이블의 컬럼 집합을 지연 조회하여 캐시한다."""
        if self._review_columns is not None:
            return self._review_columns
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SHOW COLUMNS FROM reviews")
            rows = await cur.fetchall()
        self._review_columns = {row["Field"] for row in rows or []}
        return self._review_columns

    async def find_cowatched_candidates(
        self,
        movie_id_1: str,
        movie_id_2: str,
        rating_threshold: float = 3.5,
        top_k: int = 20,
        min_co_users: int = 2,
    ) -> list[dict]:
        """
        두 영화를 모두 `rating_threshold` 이상으로 평가한 사용자가
        그 외에 높게 평가한 영화를 co-user 수 기준 상위 top_k 개 반환한다.

        Args:
            movie_id_1: 첫 번째 영화 ID
            movie_id_2: 두 번째 영화 ID
            rating_threshold: "좋아함" 으로 판단할 최소 평점 (기본 3.5/5.0)
            top_k: 최대 반환 개수
            min_co_users: 최소 공통 사용자 수 (적은 신호 영화는 제외)

        Returns:
            [{"movie_id": str, "co_user_count": int, "avg_rating": float}, ...]
            공통 사용자가 아무도 없거나 쿼리 실패 시 빈 리스트.
        """
        # 동일 movie_id 페어 가드 — 의미 없으므로 즉시 빈 결과
        if movie_id_1 == movie_id_2:
            return []

        review_cols = await self._get_review_columns()
        # soft-delete/블라인드 필터 — 컬럼이 있을 때만 적용
        extra_where = []
        if "is_deleted" in review_cols:
            extra_where.append("COALESCE(r.is_deleted, 0) = 0")
        if "is_blinded" in review_cols:
            extra_where.append("COALESCE(r.is_blinded, 0) = 0")
        active_filter_r = (" AND " + " AND ".join(extra_where)) if extra_where else ""

        # co_users subquery 에도 동일 필터 적용 (r 별칭은 동일 문자열 재사용 가능)
        # 두 영화를 모두 좋아한 공통 사용자: INNER JOIN 기반
        sql = f"""
            SELECT
                r.movie_id        AS movie_id,
                COUNT(DISTINCT r.user_id) AS co_user_count,
                AVG(r.rating)     AS avg_rating
            FROM reviews r
            INNER JOIN (
                -- 두 영화 모두 rating >= threshold 로 평가한 사용자 집합
                SELECT r1.user_id
                FROM reviews r1
                INNER JOIN reviews r2
                    ON r1.user_id = r2.user_id
                WHERE r1.movie_id = %s
                  AND r1.rating >= %s
                  AND r2.movie_id = %s
                  AND r2.rating >= %s
                  {active_filter_r.replace("r.", "r1.")}
                  {active_filter_r.replace("r.", "r2.")}
                GROUP BY r1.user_id
            ) co_users
                ON r.user_id = co_users.user_id
            WHERE r.movie_id NOT IN (%s, %s)
              AND r.rating >= %s
              {active_filter_r}
            GROUP BY r.movie_id
            HAVING co_user_count >= %s
            ORDER BY co_user_count DESC, avg_rating DESC
            LIMIT %s
        """
        params = (
            movie_id_1,
            rating_threshold,
            movie_id_2,
            rating_threshold,
            movie_id_1,
            movie_id_2,
            rating_threshold,
            min_co_users,
            top_k,
        )

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        # Decimal → float 변환 (aiomysql 이 AVG 를 Decimal 로 반환)
        return [
            {
                "movie_id": row["movie_id"],
                "co_user_count": int(row["co_user_count"] or 0),
                "avg_rating": float(row["avg_rating"] or 0.0),
            }
            for row in rows or []
        ]
