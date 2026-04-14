"""
Movie Match — Co-watched CF API 엔드포인트 (v2 Raw SQL).

Agent Movie Match (monglepick-agent) 의 rag_retriever 가 RRF 병합용 후보 소스로 호출한다.
사용자 인증 불필요 — ServiceKey 기반 내부 호출 또는 Nginx IP 화이트리스트로 보호 권장.
"""

from __future__ import annotations

import logging

import aiomysql
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.v2.api.deps import get_conn, get_redis_client
from app.v2.service.match_cowatch_service import MatchCowatchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/match", tags=["Movie Match CF (v2)"])


# ============================================================
# Request / Response 스키마
# ============================================================

class CoWatchedRequest(BaseModel):
    """Co-watched CF 조회 요청."""

    movie_id_1: str = Field(..., min_length=1, description="첫 번째 영화 ID")
    movie_id_2: str = Field(..., min_length=1, description="두 번째 영화 ID")
    top_k: int = Field(default=20, ge=1, le=50, description="최대 반환 개수")
    rating_threshold: float = Field(
        default=3.5,
        ge=0.0,
        le=5.0,
        description='"좋아함" 으로 판단할 최소 평점 (5점 만점)',
    )


class CoWatchedMovie(BaseModel):
    """Co-watched CF 결과 단일 항목."""

    movie_id: str = Field(..., description="영화 ID")
    co_user_count: int = Field(..., description="두 영화 모두 좋아한 공통 사용자 수")
    avg_rating: float = Field(..., description="해당 영화에 공통 사용자들이 매긴 평균 평점")
    cf_score: float = Field(..., description="0~1 정규화 CF 점수 (RRF 입력용)")


class CoWatchedResponse(BaseModel):
    """Co-watched CF 조회 응답."""

    movies: list[CoWatchedMovie] = Field(default_factory=list)
    total: int = Field(..., description="반환된 영화 수")


# ============================================================
# 엔드포인트
# ============================================================

@router.post(
    "/co-watched",
    response_model=CoWatchedResponse,
    summary='"둘 다 좋아한 사용자" 기반 CF 후보 영화 조회',
    description="""
    Movie Match 의 "둘이 영화 고르기" 기능에서 사용하는 협업 필터링 후보 소스.

    두 영화를 모두 `rating_threshold` 이상으로 평가한 사용자가, 그 외에 높게 평가한 영화를
    공통 사용자 수 기준 상위 `top_k` 개 반환한다.

    이 결과는 Agent rag_retriever 에서 Qdrant/ES/Neo4j 하이브리드 검색 결과와
    RRF(k=60)로 병합되어 최종 후보가 된다.
    """,
)
async def get_cowatched_movies(
    payload: CoWatchedRequest,
    conn: aiomysql.Connection = Depends(get_conn),
    redis_client: aioredis.Redis = Depends(get_redis_client),
) -> CoWatchedResponse:
    """두 영화 모두 높게 평가한 사용자의 다른 영화 목록 조회."""
    if payload.movie_id_1 == payload.movie_id_2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="movie_id_1 과 movie_id_2 는 서로 달라야 합니다.",
        )

    service = MatchCowatchService(conn=conn, redis_client=redis_client)
    try:
        rows = await service.get_cowatched_candidates(
            movie_id_1=payload.movie_id_1,
            movie_id_2=payload.movie_id_2,
            top_k=payload.top_k,
            rating_threshold=payload.rating_threshold,
        )
    except Exception as e:
        # 서비스 레이어가 빈 리스트로 graceful fallback 하지만,
        # 치명 예외가 밖으로 나올 경우 500 대신 빈 결과로 응답해 에이전트를 보호한다.
        logger.error("match_cowatched_api_error %s", e)
        rows = []

    return CoWatchedResponse(
        movies=[CoWatchedMovie(**r) for r in rows],
        total=len(rows),
    )


@router.get(
    "/co-watched",
    response_model=CoWatchedResponse,
    summary='"둘 다 좋아한 사용자" CF 조회 (GET 호환, 테스트용)',
)
async def get_cowatched_movies_by_query(
    movie_id_1: str = Query(..., description="첫 번째 영화 ID"),
    movie_id_2: str = Query(..., description="두 번째 영화 ID"),
    top_k: int = Query(default=20, ge=1, le=50),
    rating_threshold: float = Query(default=3.5, ge=0.0, le=5.0),
    conn: aiomysql.Connection = Depends(get_conn),
    redis_client: aioredis.Redis = Depends(get_redis_client),
) -> CoWatchedResponse:
    """GET 호환 엔드포인트 — 수동 테스트 및 간단 integration 용."""
    return await get_cowatched_movies(
        payload=CoWatchedRequest(
            movie_id_1=movie_id_1,
            movie_id_2=movie_id_2,
            top_k=top_k,
            rating_threshold=rating_threshold,
        ),
        conn=conn,
        redis_client=redis_client,
    )
