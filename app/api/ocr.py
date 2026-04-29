"""
OCR 영수증 분석 API

엔드포인트:
    POST /api/v1/ocr/analyze       — 이미지 URL → 전체 OCR + 파싱
    POST /api/v1/ocr/debug-parse   — 텍스트 직접 입력 → 파싱 결과 + 라인별 원문
                                     (운영 환경에서는 등록되지 않음)
    POST /api/v1/ocr/debug-ocr     — 이미지 URL → OCR 원문 + 파싱 결과 (디버그용)
                                     (운영 환경에서는 등록되지 않음)

보안:
    debug-* 엔드포인트는 ENV 환경변수가 "production"/"prod" 인 경우
    라우터에 등록되지 않는다. 운영 배포 시 .env 의 ENV=production 을 반드시
    설정할 것. 로컬/스테이징에서는 그대로 노출되어 파서 디버깅을 지원한다.
"""

import difflib
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.model.schema import OcrAnalyzeRequest, OcrAnalyzeResponse
from app.service.ocr_service import extract_text_from_url
from app.service.receipt_parser_service import parse_receipt, _normalize_ocr_text, _split_lines

logger = logging.getLogger(__name__)

# ENV 가 production/prod 인 경우 debug EP 를 등록하지 않는다.
# "운영에서는 원문 텍스트와 파서 내부 상태가 외부로 노출되어선 안 된다"
# 는 보안 원칙에 따른 가드. 스테이징/로컬은 기본 편의 유지.
_IS_PRODUCTION: bool = os.getenv("ENV", "").lower() in {"production", "prod"}

router = APIRouter(prefix="/ocr", tags=["OCR 영수증 분석"])


# ── 이벤트 컨텍스트 조회 ──────────────────────────────

async def _fetch_event(db: AsyncSession, event_id: str) -> Optional[dict]:
    """ocr_event 테이블에서 이벤트 메타 조회."""
    try:
        result = await db.execute(
            text("SELECT title, movie_id, start_date, end_date FROM ocr_event WHERE event_id = :eid"),
            {"eid": int(event_id)},
        )
        row = result.fetchone()
        if row:
            return {"title": row[0], "movie_id": row[1], "start_date": row[2], "end_date": row[3]}
    except Exception as e:
        logger.warning("ocr_event 조회 실패 event_id=%s: %s", event_id, e)
    return None


def _movie_similarity(a: str, b: str) -> float:
    """두 영화 제목의 유사도 (0.0~1.0). 공백·특수문자 제거 후 비교."""
    def norm(s: str) -> str:
        return re.sub(r"[\s\-:·：··]", "", s).lower()

    na, nb = norm(a), norm(b)
    if not na or not nb:
        return 0.0
    if na in nb or nb in na:
        return 0.85
    return difflib.SequenceMatcher(None, na, nb).ratio()


def _adjust_confidence(
    confidence: float,
    extracted_movie: Optional[str],
    extracted_date: Optional[str],
    event_title: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> float:
    """이벤트 제목·기간과 비교해 신뢰도를 보정한다."""
    adj = 0.0

    # 영화 제목 유사도 검증
    if event_title:
        if extracted_movie:
            sim = _movie_similarity(extracted_movie, event_title)
            logger.info("영화명 유사도 — extracted=%s event=%s sim=%.2f", extracted_movie, event_title, sim)
            if sim >= 0.6:
                adj += 0.10      # 제목 일치 → 소폭 보너스
            elif sim >= 0.35:
                adj -= 0.20      # 부분 불일치
            else:
                adj -= 0.50      # 다른 영화 → 큰 패널티
        else:
            adj -= 0.30          # 제목 추출 자체 실패

    # 관람일 기간 검증
    if extracted_date and start_date and end_date:
        try:
            watch = datetime.strptime(extracted_date, "%Y-%m-%d").date()
            if start_date.date() <= watch <= end_date.date():
                adj += 0.10      # 이벤트 기간 내
            else:
                logger.info("관람일 범위 초과 — date=%s period=%s~%s", watch, start_date.date(), end_date.date())
                adj -= 0.30      # 이벤트 기간 외
        except (ValueError, AttributeError):
            pass

    result = round(max(0.0, min(1.0, confidence + adj)), 2)
    logger.info("신뢰도 보정 — base=%.2f adj=%.2f final=%.2f", confidence, adj, result)
    return result


# ── 분석 엔드포인트 ───────────────────────────────────

@router.post("/analyze", response_model=OcrAnalyzeResponse, summary="영수증 OCR 분석")
async def analyze_receipt(
    request: OcrAnalyzeRequest,
    db: AsyncSession = Depends(get_db),
) -> OcrAnalyzeResponse:
    """
    영수증 이미지 URL을 받아 OCR 분석 후 구조화된 데이터를 반환한다.

    - best_text: 4가지 전처리 변형 중 가장 점수 높은 텍스트 → 영화명·관람일 파싱
    - all_texts: 모든 변형 텍스트 → 인원 수 폴백 추출 (메인 텍스트에서 실패 시)
    - 개별 필드(movie_name_ok / watch_date_ok / headcount_ok)로 부분 성공을 명시한다.
    """
    logger.info("OCR 분석 요청 — event_id=%s url=%s", request.event_id, request.image_url)

    # 이벤트 메타 조회 (제목·기간 검증용)
    event = None
    if request.event_id:
        event = await _fetch_event(db, request.event_id)
        if event:
            logger.info("이벤트 컨텍스트 — title=%s period=%s~%s",
                        event["title"], event["start_date"], event["end_date"])

    best_text, all_texts = await extract_text_from_url(request.image_url)

    if best_text is None:
        logger.warning("OCR 텍스트 추출 실패 — url=%s", request.image_url)
        return OcrAnalyzeResponse(
            success=False,
            confidence=0.0,
            error_message="이미지에서 텍스트를 추출할 수 없습니다. 이미지를 확인해주세요.",
        )

    result = parse_receipt(best_text, fallback_texts=all_texts)

    # 이벤트 제목·기간 기준으로 신뢰도 보정
    confidence = result["confidence"]
    if event:
        confidence = _adjust_confidence(
            confidence,
            extracted_movie=result["movie_name"],
            extracted_date=result["watch_date"],
            event_title=event["title"],
            start_date=event["start_date"],
            end_date=event["end_date"],
        )

    logger.info(
        "OCR 분석 완료 — status=%s movie_ok=%s date_ok=%s headcount_ok=%s "
        "seat_ok=%s time_ok=%s theater_ok=%s venue_ok=%s confidence=%.2f",
        result["status"], result["movie_name_ok"], result["watch_date_ok"], result["headcount_ok"],
        result["seat_ok"], result["screening_time_ok"], result["theater_ok"],
        result["venue_ok"], confidence,
    )

    return OcrAnalyzeResponse(
        success=True,
        status=result["status"],
        movie_name=result["movie_name"],
        watch_date=result["watch_date"],
        headcount=result["headcount"],
        seat=result["seat"],
        screening_time=result["screening_time"],
        theater=result["theater"],
        venue=result["venue"],
        watched_at=result["watched_at"],
        parsed_text=best_text,
        confidence=confidence,
        movie_name_ok=result["movie_name_ok"],
        watch_date_ok=result["watch_date_ok"],
        headcount_ok=result["headcount_ok"],
        seat_ok=result["seat_ok"],
        screening_time_ok=result["screening_time_ok"],
        theater_ok=result["theater_ok"],
        venue_ok=result["venue_ok"],
    )


# ── 디버그 엔드포인트 ─────────────────────────────────

class DebugParseRequest(BaseModel):
    text: str
    fallback_texts: Optional[List[str]] = None


class DebugParseResponse(BaseModel):
    lines: List[str]
    normalized_text: str
    parse_result: Dict[str, Any]


class DebugOcrRequest(BaseModel):
    image_url: str
    event_id: Optional[str] = None


class DebugOcrResponse(BaseModel):
    best_text: Optional[str]
    all_variant_texts: List[str]
    lines: List[str]
    parse_result: Dict[str, Any]


# debug 엔드포인트: 운영 환경에서는 라우터 등록 자체를 건너뛴다.
# FastAPI 라우팅은 모듈 import 시 결정되므로, 운영 배포 시 ENV=production
# 을 설정하면 OpenAPI 스펙에도 노출되지 않는다.
if not _IS_PRODUCTION:

    @router.post("/debug-parse", response_model=DebugParseResponse, summary="텍스트 직접 파싱 테스트")
    def debug_parse(request: DebugParseRequest) -> DebugParseResponse:
        """
        OCR 없이 텍스트를 직접 입력해 파싱 결과를 확인한다.
        영수증 원문을 붙여넣어 어떤 필드가 추출되는지 즉시 검증할 수 있다.
        """
        normalized = _normalize_ocr_text(request.text)
        lines = _split_lines(normalized)
        result = parse_receipt(request.text, fallback_texts=request.fallback_texts)
        return DebugParseResponse(
            lines=lines,
            normalized_text=normalized,
            parse_result=result,
        )

    @router.post("/debug-ocr", response_model=DebugOcrResponse, summary="이미지 OCR 원문 + 파싱 디버그")
    async def debug_ocr(request: DebugOcrRequest) -> DebugOcrResponse:
        """
        이미지 URL을 OCR한 뒤 원문 텍스트와 파싱 결과를 모두 반환한다.
        admin에서 '왜 이 필드가 안 잡히나?' 디버깅용으로 사용한다.
        """
        logger.info("OCR 디버그 요청 — url=%s", request.image_url)
        best_text, all_texts = await extract_text_from_url(request.image_url)

        if best_text is None:
            return DebugOcrResponse(
                best_text=None,
                all_variant_texts=all_texts,
                lines=[],
                parse_result={"error": "OCR 텍스트 추출 실패"},
            )

        normalized = _normalize_ocr_text(best_text)
        lines = _split_lines(normalized)
        result = parse_receipt(best_text, fallback_texts=all_texts)

        return DebugOcrResponse(
            best_text=best_text,
            all_variant_texts=all_texts,
            lines=lines,
            parse_result=result,
        )
else:
    logger.info("OCR debug 엔드포인트 비활성화 — ENV=%s", os.getenv("ENV"))