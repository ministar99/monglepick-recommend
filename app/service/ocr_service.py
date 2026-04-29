"""
영화관 영수증 OCR 서비스 (Upstage Document AI)

이미지 URL 다운로드 → Upstage OCR API 호출 → 텍스트 반환

보안 정책:
  - SSRF 방어: image_url 은 http/https 만 허용하고, 호스트 해석 결과가 사설/
    루프백/링크로컬/예약 대역이면 거부한다. 운영 환경은 _ALLOWED_HOSTS
    환경변수(쉼표 구분)로 업로드 도메인만 허용하도록 구성할 수 있다.
  - 이미지 크기 제한: 다운로드 스트림을 청크 단위로 누적하며 _MAX_IMAGE_BYTES
    (기본 10MB) 초과 시 즉시 중단하여 메모리 폭발/DoS 를 방지한다.
"""
import io
import os
import re
import socket
import ipaddress
import logging
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 보안 설정 상수
# ──────────────────────────────────────────────

_ALLOWED_SCHEMES: frozenset[str] = frozenset({"http", "https"})
_MAX_IMAGE_BYTES: int = int(os.getenv("OCR_MAX_IMAGE_BYTES", 10 * 1024 * 1024))
_DOWNLOAD_TIMEOUT: float = float(os.getenv("OCR_DOWNLOAD_TIMEOUT", 15.0))
_ALLOWED_HOSTS: frozenset[str] = frozenset(
    h.strip().lower() for h in os.getenv("OCR_ALLOWED_HOSTS", "").split(",") if h.strip()
)

# ──────────────────────────────────────────────
# Upstage OCR 설정
# ──────────────────────────────────────────────

_UPSTAGE_API_KEY: str = os.getenv("UPSTAGE_API_KEY", "")
_UPSTAGE_OCR_URL: str = "https://api.upstage.ai/v1/document-ai/ocr"
_UPSTAGE_TIMEOUT: float = float(os.getenv("UPSTAGE_OCR_TIMEOUT", 30.0))


# ──────────────────────────────────────────────
# OCR 품질 점수 (로깅용)
# ──────────────────────────────────────────────

_SCORE_PATTERNS: List[Tuple[str, float]] = [
    (r"(?:CGV|메가박스|MEGABOX|롯데\s*시네마|LOTTE\s*CINEMA|B[O0]X\s*KIOSK)", 30.0),
    (r"(?:전체|12세|15세|18세|청소년).{0,4}관람",                               25.0),
    (r"(?:일반|성인|청소년|우대|군인)\s*\d+\s*[명매]",                           20.0),
    (r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",                                        20.0),
    (r"\d{2}[./-]\d{1,2}[./-]\d{1,2}",                                        15.0),
    (r"\d{1,2}:\d{2}",                                                         10.0),
    (r"[A-Z가-힣]\s*열\s*\d+\s*번?",                                           20.0),
    (r"\b[A-Z]\d{1,3}\b",                                                      12.0),
    (r"\d+\s*관(?!람|객)",                                                      12.0),
    (r"(?:영화명|작품명|상영\s*제목)",                                            18.0),
    (r"(?:좌석\s*번호?|SEAT\b)",                                                15.0),
    (r"(?:상영관|관람관|관람일|관람일시|상영일시)",                                 12.0),
    (r"(?:영화|관람|상영|티켓|입장|좌석)",                                         8.0),
    (r"(?:어벤|아바타|오펜하이|파묘|범죄|Avengers|Avatar|Oppenheimer)",           35.0),
]


def _ocr_score(text: str) -> float:
    if not text:
        return 0.0
    score = min(len(text) * 0.3, 80.0)
    for pattern, boost in _SCORE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            score += boost
    return score


# ──────────────────────────────────────────────
# Upstage OCR 호출
# ──────────────────────────────────────────────

async def _call_upstage_ocr(image_bytes: bytes, filename: str = "receipt.jpg") -> Optional[str]:
    if not _UPSTAGE_API_KEY:
        raise RuntimeError("UPSTAGE_API_KEY 환경변수가 설정되지 않았습니다.")

    headers = {"Authorization": f"Bearer {_UPSTAGE_API_KEY}"}
    files = {"document": (filename, io.BytesIO(image_bytes), "image/jpeg")}

    async with httpx.AsyncClient(timeout=_UPSTAGE_TIMEOUT) as client:
        response = await client.post(_UPSTAGE_OCR_URL, headers=headers, files=files)
        response.raise_for_status()

    data = response.json()

    # 최상위 text 필드가 전 페이지 합산 텍스트. 없으면 pages[].text 폴백.
    text = data.get("text") or ""
    if not text:
        pages = data.get("pages") or []
        text = "\n".join(p.get("text", "") for p in pages if p.get("text"))
    return text or None


# ──────────────────────────────────────────────
# 보안 — SSRF 방어 + 스트리밍 크기 제한
# ──────────────────────────────────────────────

class UnsafeImageUrlError(ValueError):
    """SSRF 방어에 의해 거부된 URL 입력 오류."""


def _validate_image_url(image_url: str) -> str:
    parsed = urlparse(image_url)

    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        raise UnsafeImageUrlError(f"허용되지 않는 스킴: {parsed.scheme!r}")

    host = (parsed.hostname or "").lower()
    if not host:
        raise UnsafeImageUrlError("호스트명이 비어 있습니다")

    if _ALLOWED_HOSTS and host not in _ALLOWED_HOSTS:
        raise UnsafeImageUrlError(f"허용되지 않은 호스트: {host!r}")

    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise UnsafeImageUrlError(f"호스트 해석 실패: {host!r} ({e})") from e

    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise UnsafeImageUrlError(
                f"내부/예약 IP 대역은 허용되지 않습니다: {host!r} → {ip_str}"
            )

    return image_url


async def _download_image_bytes(image_url: str) -> Optional[bytes]:
    try:
        async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT, follow_redirects=False) as client:
            async with client.stream("GET", image_url) as response:
                response.raise_for_status()

                content_length = response.headers.get("content-length")
                if content_length is not None:
                    try:
                        if int(content_length) > _MAX_IMAGE_BYTES:
                            logger.warning(
                                "이미지 크기 초과(선언) — Content-Length=%s limit=%d",
                                content_length, _MAX_IMAGE_BYTES,
                            )
                            return None
                    except ValueError:
                        pass

                buf = bytearray()
                async for chunk in response.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > _MAX_IMAGE_BYTES:
                        logger.warning(
                            "이미지 크기 초과(스트림) — 누적=%d limit=%d",
                            len(buf), _MAX_IMAGE_BYTES,
                        )
                        return None
                return bytes(buf)
    except httpx.HTTPError as e:
        logger.error("이미지 다운로드 실패 url=%s error=%s", image_url, e)
        return None


# ──────────────────────────────────────────────
# 공개 API
# ──────────────────────────────────────────────

def re_search_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(p, text) for p in patterns)


async def extract_text_from_url(image_url: str) -> Tuple[Optional[str], List[str]]:
    try:
        _validate_image_url(image_url)
    except UnsafeImageUrlError as e:
        logger.warning("OCR URL 거부 — %s", e)
        return None, []

    image_bytes = await _download_image_bytes(image_url)
    if not image_bytes:
        return None, []

    try:
        parsed = urlparse(image_url)
        filename = os.path.basename(parsed.path) or "receipt.jpg"

        text = await _call_upstage_ocr(image_bytes, filename)
        if not text:
            logger.warning("Upstage OCR 추출 결과 없음")
            return None, []

        score = _ocr_score(text)
        logger.info("Upstage OCR 완료 — score=%.1f  chars=%d", score, len(text))
        return text, [text]
    except Exception as e:
        logger.error("OCR 처리 오류: %s", e)
        return None, []
