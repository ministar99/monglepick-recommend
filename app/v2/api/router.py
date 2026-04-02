"""
v2 API 라우터 통합 모듈

모든 v2 API 엔드포인트를 /api/v2 접두어 아래에 통합합니다.
v1(/api/v1)과 병렬 운영하여 A/B 비교 테스트를 지원합니다.

엔드포인트 그룹:
- /api/v2/search/*      → 영화 검색 (Raw SQL)
- /api/v2/onboarding/*  → 온보딩 개인화 (Raw SQL)
"""

from fastapi import APIRouter

from app.v2.api.search import router as search_router
from app.v2.api.onboarding import router as onboarding_router

# ─────────────────────────────────────────
# v2 API 라우터 생성
# /api/v2 접두어로 v1과 분리하여 병렬 운영
# ─────────────────────────────────────────
api_v2_router = APIRouter(prefix="/api/v2")

# 영화 검색 라우터 등록
api_v2_router.include_router(search_router)

# 온보딩 라우터 등록
api_v2_router.include_router(onboarding_router)
