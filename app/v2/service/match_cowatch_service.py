"""
Movie Match — Co-watched CF 서비스 (v2 Raw SQL).

Agent Movie Match 의 rag_retriever 가 호출하는 "둘 다 좋아한 사용자" 후보 목록 제공자.
리포지토리에서 조회한 결과에 Redis 캐싱(TTL 5분)과 응답 포맷 변환을 추가한다.
"""

from __future__ import annotations

import json
import logging
import time

import aiomysql
import redis.asyncio as aioredis

from app.core.metrics import (
    match_cowatch_cache_total,
    match_cowatch_endpoint_duration_seconds,
    match_cowatch_query_duration_seconds,
)
from app.v2.repository.match_cowatch_repository import MatchCowatchRepository

logger = logging.getLogger(__name__)


class MatchCowatchService:
    """Movie Match CF 후보 서비스."""

    # Redis 캐시 키 네임스페이스 — movie_id 정렬하여 순서 무관하게 동일 키 생성
    _CACHE_PREFIX = "match:cowatched"
    _CACHE_TTL = 300  # 5분 — 리뷰 데이터는 실시간성이 낮아 짧게 유지

    def __init__(self, conn: aiomysql.Connection, redis_client: aioredis.Redis | None = None):
        self._conn = conn
        self._redis = redis_client

    @staticmethod
    def _cache_key(movie_id_1: str, movie_id_2: str, top_k: int, threshold: float) -> str:
        """순서 독립적인 캐시 키 — (A,B) 와 (B,A) 는 동일 키."""
        a, b = sorted([movie_id_1, movie_id_2])
        return f"{MatchCowatchService._CACHE_PREFIX}:{a}:{b}:top{top_k}:th{threshold}"

    async def get_cowatched_candidates(
        self,
        movie_id_1: str,
        movie_id_2: str,
        top_k: int = 20,
        rating_threshold: float = 3.5,
    ) -> list[dict]:
        """
        두 영화를 모두 높게 평가한 사용자의 다른 "좋아한" 영화 목록을 반환한다.

        1) Redis 캐시 hit 시 즉시 반환
        2) 캐시 miss 시 리포지토리 호출 → 캐시 저장 → 반환
        3) 리포지토리 실패 시 빈 리스트 반환 (에이전트 graceful fallback)

        Returns:
            [{"movie_id": str, "co_user_count": int, "avg_rating": float, "cf_score": float}]
            cf_score 는 co_user_count 기반 정규화 점수 (0~1), Agent RRF 에서 활용.
        """
        cache_key = self._cache_key(movie_id_1, movie_id_2, top_k, rating_threshold)
        # Prometheus 전체 응답 시간 측정 시작 (cache hit/miss 두 경로 공통)
        endpoint_start = time.perf_counter()

        # ── [1] Redis 캐시 조회 ──
        if self._redis is not None:
            try:
                cached = await self._redis.get(cache_key)
                if cached:
                    logger.debug("match_cowatch_cache_hit key=%s", cache_key)
                    # Prometheus: 캐시 hit 카운터 + endpoint 응답시간 (cache=hit)
                    match_cowatch_cache_total.labels(outcome="hit").inc()
                    match_cowatch_endpoint_duration_seconds.labels(
                        cache="hit",
                    ).observe(time.perf_counter() - endpoint_start)
                    return json.loads(cached)
                # 캐시 miss 카운터 증가 (duration 은 최종 return 시점에)
                match_cowatch_cache_total.labels(outcome="miss").inc()
            except Exception as e:
                # 캐시 실패는 로그만 남기고 무시 (서비스 중단 방지)
                logger.warning("match_cowatch_cache_read_error %s", e)
                match_cowatch_cache_total.labels(outcome="error").inc()

        # ── [2] MySQL 조회 (duration 별도 측정) ──
        query_start = time.perf_counter()
        try:
            repo = MatchCowatchRepository(self._conn)
            rows = await repo.find_cowatched_candidates(
                movie_id_1=movie_id_1,
                movie_id_2=movie_id_2,
                rating_threshold=rating_threshold,
                top_k=top_k,
            )
            match_cowatch_query_duration_seconds.observe(
                time.perf_counter() - query_start,
            )
        except Exception as e:
            logger.error("match_cowatch_repository_error %s", e)
            # MySQL 실패해도 endpoint duration 은 기록 (메트릭 일관성)
            match_cowatch_endpoint_duration_seconds.labels(
                cache="miss",
            ).observe(time.perf_counter() - endpoint_start)
            return []

        # ── [3] 응답 포맷 변환 + cf_score 정규화 ──
        # cf_score: co_user_count 를 최대값 기준 0~1 로 정규화 + avg_rating 가중치
        # → Agent rag_retriever 에서 RRF 에 태울 때 순위 결정용 점수
        max_count = max((r["co_user_count"] for r in rows), default=1)
        enriched = [
            {
                **r,
                # 0.7: co_user_count 비중, 0.3: avg_rating 비중 (5점 만점 정규화)
                "cf_score": round(
                    0.7 * (r["co_user_count"] / max_count)
                    + 0.3 * (r["avg_rating"] / 5.0),
                    4,
                ),
            }
            for r in rows
        ]

        # ── [4] 캐시 저장 (best-effort) ──
        if self._redis is not None and enriched:
            try:
                await self._redis.setex(
                    cache_key,
                    self._CACHE_TTL,
                    json.dumps(enriched, ensure_ascii=False),
                )
            except Exception as e:
                logger.warning("match_cowatch_cache_write_error %s", e)

        # Prometheus: 캐시 miss 경로 endpoint 전체 응답 시간 기록
        match_cowatch_endpoint_duration_seconds.labels(
            cache="miss",
        ).observe(time.perf_counter() - endpoint_start)
        return enriched
