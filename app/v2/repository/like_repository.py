"""
영화 좋아요 리포지토리 (v2 Raw SQL)
=================================================================

Backend(monglepick-backend)에서 이관된 movie Like 도메인을 FastAPI(recommend)에서
Raw SQL(aiomysql)로 재구현한다.

설계 결정 (2026-04-07):
  1. 원본: monglepick-backend/domain/movie/Like.java + LikeRepository/LikeService/LikeController
  2. DDL 마스터는 Backend JPA(@Entity)이며, 이 리포지토리는 같은 `likes` 테이블을 읽고 쓴다.
  3. 정합성 모델은 "하이브리드 write-behind":
     - 사용자 요청 응답은 Redis(count/user set) 기준으로 즉시 반환
     - 실제 DB 반영은 app/background/like_flush.py 의 스케줄러가 주기적 배치 처리
  4. 소프트 삭제(deleted_at)를 그대로 유지하여 UNIQUE(user_id, movie_id) 제약을 우회하고,
     좋아요 취소 → 재활성화 시 INSERT 없이 UPDATE만으로 처리한다.

트랜잭션 정책:
  - 커밋/롤백은 상위 서비스/스케줄러가 담당한다. 이 리포지토리는 SQL 실행에 집중.
  - 다만 `apply_toggle` 계열은 배치에서 여러 개를 묶어 호출되므로 동일 커넥션 재사용을 가정한다.

DDL 컬럼 기준 (Backend JPA Like.java 2026-04-07):
  like_id      BIGINT AUTO_INCREMENT PRIMARY KEY
  user_id      VARCHAR(50) NOT NULL
  movie_id     VARCHAR(50) NOT NULL
  deleted_at   DATETIME NULL
  created_at   DATETIME NOT NULL  (BaseAuditEntity 자동)
  updated_at   DATETIME NOT NULL  (BaseAuditEntity 자동)
  created_by   VARCHAR(50) NULL   (BaseAuditEntity 자동)
  updated_by   VARCHAR(50) NULL   (BaseAuditEntity 자동)
  UNIQUE KEY uk_likes_user_movie (user_id, movie_id)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Literal, Optional

import aiomysql

from app.v2.model.dto import LikeDTO

logger = logging.getLogger(__name__)


# 토글 연산 타입: flush 큐에 기록되는 op 값과 동일하게 유지한다.
ToggleOp = Literal["LIKE", "UNLIKE"]


class LikeRepository:
    """영화 좋아요 MySQL 리포지토리 (Raw SQL)."""

    def __init__(self, conn: aiomysql.Connection):
        """
        Args:
            conn: aiomysql 비동기 커넥션 (DictCursor 기본).
                  배치 flush 시에는 lifespan 풀에서 직접 획득한 커넥션을 사용한다.
        """
        self._conn = conn

    # ─────────────────────────────────────────────────────────
    # 단건 조회 — Redis 리하이드레이션 및 서비스 레이어 동기 API
    # ─────────────────────────────────────────────────────────

    async def find_by_user_movie(self, user_id: str, movie_id: str) -> Optional[LikeDTO]:
        """
        (user_id, movie_id)로 좋아요 레코드를 조회한다.

        Backend LikeRepository.findByUserIdAndMovieId와 1:1 대응.
        soft-delete 여부와 관계없이 레코드 자체를 반환한다 — 호출자는
        `LikeDTO.is_active()` 또는 `dto.deleted_at is None`으로 활성 여부를 판단한다.
        """
        sql = (
            "SELECT like_id, user_id, movie_id, deleted_at, "
            "       created_at, updated_at, created_by, updated_by "
            "FROM likes "
            "WHERE user_id = %s AND movie_id = %s "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id, movie_id))
            row = await cur.fetchone()
        return LikeDTO(**row) if row else None

    async def count_active_by_movie(self, movie_id: str) -> int:
        """
        특정 영화의 활성 좋아요 수를 반환한다 (deleted_at IS NULL).

        Backend LikeRepository.countByMovieIdAndDeletedAtIsNull과 1:1 대응.
        Redis `like:count:{movie_id}` 캐시 미스 시 이 메서드로 초기화한다.
        """
        sql = (
            "SELECT COUNT(*) AS cnt "
            "FROM likes "
            "WHERE movie_id = %s AND deleted_at IS NULL"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (movie_id,))
            row = await cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0

    async def list_active_movie_ids_by_user(self, user_id: str) -> list[str]:
        """
        사용자의 활성 좋아요 영화 ID 목록을 반환한다 (deleted_at IS NULL).

        Redis `like:user:{user_id}` SET 캐시 리하이드레이션 용도.
        """
        sql = (
            "SELECT movie_id "
            "FROM likes "
            "WHERE user_id = %s AND deleted_at IS NULL"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (user_id,))
            rows = await cur.fetchall()
        return [row["movie_id"] for row in rows] if rows else []

    # ─────────────────────────────────────────────────────────
    # 토글 반영 — write-behind 스케줄러가 큐에서 꺼내 호출
    # ─────────────────────────────────────────────────────────

    async def apply_toggle(
        self,
        user_id: str,
        movie_id: str,
        op: ToggleOp,
    ) -> LikeDTO:
        """
        단건 좋아요 토글을 DB에 반영한다.

        호출 규약 (스케줄러 / 긴급 동기 flush 양쪽 공용):
          - op="LIKE"  : 활성 좋아요 상태로 수렴한다
              * 레코드 없음        → INSERT (deleted_at=NULL)
              * 레코드 있음 + 활성 → no-op (이미 활성)
              * 레코드 있음 + 취소 → UPDATE deleted_at=NULL (restore)
          - op="UNLIKE": 취소 상태로 수렴한다
              * 레코드 없음        → INSERT + 바로 deleted_at 설정 (사실상 드물지만 정합성)
              * 레코드 있음 + 활성 → UPDATE deleted_at=now (soft delete)
              * 레코드 있음 + 취소 → no-op

        여러 번 호출되어도 같은 최종 상태로 수렴(idempotent)하므로,
        flush 재시도/중복 실행에 안전하다.

        Returns:
            반영 후 레코드 DTO
        """
        existing = await self.find_by_user_movie(user_id, movie_id)
        now = datetime.now(timezone.utc)

        if op == "LIKE":
            if existing is None:
                # 신규 INSERT — deleted_at=NULL
                return await self._insert_active(user_id, movie_id, now)
            if existing.deleted_at is None:
                # 이미 활성 — no-op
                logger.debug(
                    "apply_toggle LIKE no-op (이미 활성): user=%s movie=%s",
                    user_id, movie_id,
                )
                return existing
            # 취소된 상태 → 복구
            return await self._update_deleted_at(existing.like_id, None, now)

        # op == "UNLIKE"
        if existing is None:
            # 드문 케이스: 큐에 UNLIKE만 있는데 DB 레코드는 없음
            # (예: 사용자가 LIKE→UNLIKE 빠르게 토글했는데 LIKE flush가 누락된 경우)
            # INSERT + 즉시 soft delete 로 정합성 맞추기
            logger.warning(
                "apply_toggle UNLIKE without existing row — 정합성 보정 INSERT+soft delete: "
                "user=%s movie=%s",
                user_id, movie_id,
            )
            dto = await self._insert_active(user_id, movie_id, now)
            return await self._update_deleted_at(dto.like_id, now, now)
        if existing.deleted_at is not None:
            # 이미 취소 — no-op
            logger.debug(
                "apply_toggle UNLIKE no-op (이미 취소): user=%s movie=%s",
                user_id, movie_id,
            )
            return existing
        # 활성 → soft delete
        return await self._update_deleted_at(existing.like_id, now, now)

    async def batch_apply_toggles(
        self,
        entries: list[tuple[str, str, ToggleOp]],
    ) -> int:
        """
        복수 토글을 배치 반영한다.

        write-behind 스케줄러가 Redis dirty 큐를 한 번에 드레인할 때 호출한다.
        동일 (user_id, movie_id)에 대해 여러 op가 있을 경우, 호출자가 이미 최종 op로
        합쳐서 넘긴다고 가정한다 (flush 로직에서 dict로 dedup).

        Args:
            entries: [(user_id, movie_id, op), ...]

        Returns:
            성공적으로 반영된 건수
        """
        applied = 0
        for user_id, movie_id, op in entries:
            try:
                await self.apply_toggle(user_id, movie_id, op)
                applied += 1
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "like flush 단건 실패 — user=%s movie=%s op=%s: %s",
                    user_id, movie_id, op, exc,
                )
                # 단건 실패는 전체 배치 중단 없이 다음 항목으로 진행
                continue
        return applied

    # ─────────────────────────────────────────────────────────
    # 내부 헬퍼
    # ─────────────────────────────────────────────────────────

    async def _insert_active(
        self,
        user_id: str,
        movie_id: str,
        now: datetime,
    ) -> LikeDTO:
        """
        활성 상태로 신규 INSERT. BaseAuditEntity 타임스탬프 4종을 명시 세팅한다.

        Backend가 JPA @EntityListeners(AuditingEntityListener.class)로 자동 세팅하는
        부분을 Raw SQL에서는 수동으로 채워 준다.
        created_by/updated_by는 현재 서비스에서 식별 가능한 값이 없으므로
        `"recommend-like-service"`로 하드코딩한다(이관 출처 추적 목적).
        """
        insert_sql = (
            "INSERT INTO likes "
            "(user_id, movie_id, deleted_at, created_at, updated_at, created_by, updated_by) "
            "VALUES (%s, %s, NULL, %s, %s, %s, %s)"
        )
        created_by = "recommend-like-service"
        async with self._conn.cursor() as cur:
            await cur.execute(
                insert_sql,
                (user_id, movie_id, now, now, created_by, created_by),
            )
            new_id = cur.lastrowid
        return LikeDTO(
            like_id=new_id,
            user_id=user_id,
            movie_id=movie_id,
            deleted_at=None,
            created_at=now,
            updated_at=now,
            created_by=created_by,
            updated_by=created_by,
        )

    async def _update_deleted_at(
        self,
        like_id: int,
        deleted_at: Optional[datetime],
        now: datetime,
    ) -> LikeDTO:
        """
        단일 레코드의 deleted_at을 변경한다 (soft delete 또는 restore).

        updated_at/updated_by도 함께 갱신하여 audit trail 유지.
        최종 상태 레코드를 다시 조회하여 반환한다 (created_at 등 다른 필드 보존).
        """
        update_sql = (
            "UPDATE likes "
            "SET deleted_at = %s, updated_at = %s, updated_by = %s "
            "WHERE like_id = %s"
        )
        updated_by = "recommend-like-service"
        async with self._conn.cursor() as cur:
            await cur.execute(update_sql, (deleted_at, now, updated_by, like_id))

        # 최종 상태 재조회 (created_at 등 보존)
        select_sql = (
            "SELECT like_id, user_id, movie_id, deleted_at, "
            "       created_at, updated_at, created_by, updated_by "
            "FROM likes WHERE like_id = %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(select_sql, (like_id,))
            row = await cur.fetchone()
        if row is None:
            # 극히 드문 케이스: 방금 UPDATE한 행이 사라짐(DELETE 경합 등)
            # 최소한의 DTO라도 만들어 반환
            return LikeDTO(
                like_id=like_id,
                user_id="",
                movie_id="",
                deleted_at=deleted_at,
                updated_at=now,
                updated_by=updated_by,
            )
        return LikeDTO(**row)
