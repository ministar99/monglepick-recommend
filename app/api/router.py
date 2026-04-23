"""
API 라우터 통합 모듈

모든 API 엔드포인트를 /api/v1 접두어 아래에 통합합니다.

엔드포인트 그룹:
- /api/v1/search/*      → 영화 검색 (REQ_031~034)
- /api/v1/onboarding/*  → 온보딩 개인화 (REQ_016~019)
"""

from fastapi import APIRouter

from app.api.search import router as search_router
from app.api.onboarding import router as onboarding_router
from app.api.ocr import router as ocr_router

# ─────────────────────────────────────────
# v1 API 라우터 생성
# 모든 하위 라우터를 /api/v1 접두어로 통합
# ─────────────────────────────────────────
api_router = APIRouter(prefix="/api/v1")

# 영화 검색 라우터 등록
api_router.include_router(search_router)

# 온보딩 라우터 등록
api_router.include_router(onboarding_router)

# OCR 영수증 분석 라우터 등록
api_router.include_router(ocr_router)
