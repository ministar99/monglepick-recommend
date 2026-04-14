"""
v2 API 라우터 통합 모듈

모든 v2 API 엔드포인트를 /api/v2 접두어 아래에 통합합니다.
v1(/api/v1)과 병렬 운영하여 A/B 비교 테스트를 지원합니다.

엔드포인트 그룹:
- /api/v2/search/*      → 영화 검색 (Raw SQL)
- /api/v2/onboarding/*  → 온보딩 개인화 (Raw SQL)
"""

from fastapi import APIRouter

from app.v2.api.like import router as like_router
from app.v2.api.match_cowatch import router as match_cowatch_router
from app.v2.api.review import movie_review_router, user_review_router
from app.v2.api.search import router as search_router
from app.v2.api.onboarding import router as onboarding_router
from app.v2.api.wishlist import router as wishlist_router

# ─────────────────────────────────────────
# v2 API 라우터 생성
# /api/v2 접두어로 v1과 분리하여 병렬 운영
# ─────────────────────────────────────────
api_v2_router = APIRouter(prefix="/api/v2")

# 영화 검색 라우터 등록
api_v2_router.include_router(search_router)

# 온보딩 라우터 등록
api_v2_router.include_router(onboarding_router)

# 영화 좋아요 라우터 등록
# 2026-04-07 이관: Backend monglepick-backend/domain/movie/LikeController → recommend(FastAPI)
# Nginx에서 /api/v1/movies/{id}/like* 경로를 이 라우터로 프록시한다.
api_v2_router.include_router(like_router)

# 리뷰 라우터 등록
api_v2_router.include_router(movie_review_router)
api_v2_router.include_router(user_review_router)

# 위시리스트 라우터 등록
api_v2_router.include_router(wishlist_router)

# Movie Match Co-watched CF 라우터 등록
# 2026-04-14 신규: Agent Movie Match 의 rag_retriever 가 RRF 병합 후보로 호출한다.
# "두 영화 모두 높게 평가한 사용자의 다른 영화" 를 MySQL reviews 테이블 기반으로 조회.
api_v2_router.include_router(match_cowatch_router)
