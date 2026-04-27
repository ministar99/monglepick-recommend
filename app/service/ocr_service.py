"""
영화관 영수증 OCR 서비스 (Tesseract)

이미지 전처리 다중 변형(gray / 자동대비 이진화 / 고대비 샤픈) →
Tesseract OCR → 점수 가장 높은 텍스트 선택

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
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

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

# Tesseract — kor+eng: 한글+영문 혼용 영수증
# psm 6: 단일 균일 블록, psm 4: 단일 컬럼 (열지 모양 영수증에 유리)
_TESSERACT_LANG = "kor+eng"
_CFG_PSM6 = "--oem 3 --psm 6"
_CFG_PSM4 = "--oem 3 --psm 4"

# 짧은 변 기준 최소 해상도 (px) — 이 미만이면 업스케일
_MIN_DIM = 800


# ──────────────────────────────────────────────
# 공통 전처리 유틸
# ──────────────────────────────────────────────

def _resize_to_target(img: Image.Image, min_dim: int = _MIN_DIM) -> Image.Image:
    w, h = img.size
    short = min(w, h)
    if short < min_dim:
        scale = min_dim / short
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    return img


def _crop_receipt_center(img: Image.Image, margin: float = 0.03) -> Image.Image:
    w, h = img.size
    return img.crop((
        int(w * margin), int(h * margin),
        int(w * (1 - margin)), int(h * (1 - margin)),
    ))


def _base_gray(img: Image.Image) -> Image.Image:
    """RGB → 노이즈 제거 회색조."""
    gray = img.convert("L")
    return gray.filter(ImageFilter.MedianFilter(size=3))


# ──────────────────────────────────────────────
# 전처리 변형 3종
# ──────────────────────────────────────────────

def _variant_gray_enhance(gray: Image.Image) -> Image.Image:
    """대비 2배 + 밝기 미세 조정 + 샤픈 — 일반 촬영 영수증."""
    out = ImageEnhance.Contrast(gray).enhance(2.0)
    out = ImageEnhance.Brightness(out).enhance(1.1)
    return out.filter(ImageFilter.SHARPEN)


def _variant_autocontrast_binary(gray: Image.Image) -> Image.Image:
    """자동 대비 → 적응형 이진화 — 형광등 그림자·불균일 조명 대응."""
    out = ImageOps.autocontrast(gray, cutoff=2)
    pixel_sum = sum(out.getdata())
    mean_val = pixel_sum / (out.width * out.height)
    # 평균 밝기보다 약간 밝은 픽셀을 흰색으로: 어두운 이미지는 threshold 낮게
    threshold = int(min(200, max(110, mean_val * 1.05)))
    return out.point(lambda x: 255 if x > threshold else 0, "L")


def _variant_high_contrast(gray: Image.Image) -> Image.Image:
    """대비 3배 + 이중 샤픈 — 흐릿하거나 연한 열지 영수증."""
    out = ImageEnhance.Contrast(gray).enhance(3.0)
    out = out.filter(ImageFilter.SHARPEN)
    return out.filter(ImageFilter.SHARPEN)


def _preprocess_variants(image_bytes: bytes) -> List[Tuple[str, Image.Image, str]]:
    """
    (variant_name, 전처리_이미지, tesseract_config) 목록 반환.
    각 variant × PSM 조합으로 최대 5회 시도.
    """
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _base_gray(img)

    return [
        ("gray_psm6",          _variant_gray_enhance(gray),        _CFG_PSM6),
        ("gray_psm4",          _variant_gray_enhance(gray),        _CFG_PSM4),
        ("autocontrast_psm6",  _variant_autocontrast_binary(gray), _CFG_PSM6),
        ("autocontrast_psm4",  _variant_autocontrast_binary(gray), _CFG_PSM4),
        ("high_contrast_psm6", _variant_high_contrast(gray),       _CFG_PSM6),
    ]


# ──────────────────────────────────────────────
# OCR 품질 점수
# ──────────────────────────────────────────────

_SCORE_PATTERNS: List[Tuple[str, float]] = [
    # 영화관 브랜드
    (r"(?:CGV|메가박스|MEGABOX|롯데\s*시네마|LOTTE\s*CINEMA|B[O0]X\s*KIOSK)", 30.0),
    # 관람등급
    (r"(?:전체|12세|15세|18세|청소년).{0,4}관람",                               25.0),
    # 인원/좌석 레이블
    (r"(?:일반|성인|청소년|우대|군인)\s*\d+\s*[명매]",                           20.0),
    # 날짜
    (r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",                                        20.0),
    (r"\d{2}[./-]\d{1,2}[./-]\d{1,2}",                                        15.0),
    # 시간
    (r"\d{1,2}:\d{2}",                                                         10.0),
    # 좌석 (열/번 형식)
    (r"[A-Z가-힣]\s*열\s*\d+\s*번?",                                           20.0),
    # 좌석 코드 (A10, G8 등)
    (r"\b[A-Z]\d{1,3}\b",                                                      12.0),
    # 상영관
    (r"\d+\s*관(?!람|객)",                                                      12.0),
    # 영화/좌석/상영 관련 키워드 레이블
    (r"(?:영화명|작품명|상영\s*제목)",                                            18.0),
    (r"(?:좌석\s*번호?|SEAT\b)",                                                15.0),
    (r"(?:상영관|관람관|관람일|관람일시|상영일시)",                                 12.0),
    # 일반 키워드
    (r"(?:영화|관람|상영|티켓|입장|좌석)",                                         8.0),
    # 영화 제목 힌트
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
# 메인 OCR 실행
# ──────────────────────────────────────────────

def _ocr_with_best_variant(image_bytes: bytes) -> Tuple[Optional[str], List[str]]:
    variants = _preprocess_variants(image_bytes)
    best_text: Optional[str] = None
    best_score: float = -1.0
    all_texts: List[str] = []

    for name, img, cfg in variants:
        try:
            text = pytesseract.image_to_string(img, lang=_TESSERACT_LANG, config=cfg)
            text = text.strip()
        except Exception as e:
            logger.warning("OCR 실패 variant=%s error=%s", name, e)
            continue

        score = _ocr_score(text)
        logger.info(
            "── variant=%-22s score=%6.1f  chars=%4d  preview=%s",
            name, score, len(text), text[:80].replace("\n", " | "),
        )

        if text:
            all_texts.append(text)

        if score > best_score:
            best_score = score
            best_text = text

    logger.info("────────────────────────────────────────────")
    if not best_text:
        return None, all_texts

    logger.info("최선 variant 선택: score=%.1f  chars=%d", best_score, len(best_text))
    return best_text, all_texts


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
        best_text, all_texts = _ocr_with_best_variant(image_bytes)
        if not best_text:
            logger.warning("OCR 추출 결과 없음")
            return None, []
        logger.info("OCR 추출 완료 — 글자 수: %d", len(best_text))
        return best_text, all_texts
    except Exception as e:
        logger.error("OCR 처리 오류: %s", e)
        return None, []