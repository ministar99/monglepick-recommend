"""
영화관 영수증 OCR 서비스

이미지 전처리(gray 단일 변형) → PaddleOCR → 텍스트 추출
gray 전처리: 회색조 + 중간 대비 + 노이즈 제거 + 샤픈

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
import threading
from typing import Optional, List, Tuple
from urllib.parse import urlparse

# PaddleOCR 시작 시 모델 서버 연결 체크를 건너뜀 (시작 속도 향상)
os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
# PaddleOCR 3.4.x + PaddlePaddle 3.3.x PIR/oneDNN 충돌 방지
# ConvertPirAttribute / onednn_instruction.cc 오류를 막기 위해 import 전에 설정
os.environ.setdefault("FLAGS_use_mkldnn", "0")
os.environ.setdefault("FLAGS_enable_pir_api", "0")

import numpy as np
import httpx
from PIL import Image, ImageEnhance, ImageFilter
from paddleocr import PaddleOCR

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 보안 설정 상수
# ──────────────────────────────────────────────

# SSRF 방어용 허용 스킴 — file://, gopher://, dict:// 등 전면 차단.
_ALLOWED_SCHEMES: frozenset[str] = frozenset({"http", "https"})

# 이미지 다운로드 최대 크기 (바이트) — 기본 10MB. 초과 시 즉시 연결 중단.
_MAX_IMAGE_BYTES: int = int(os.getenv("OCR_MAX_IMAGE_BYTES", 10 * 1024 * 1024))

# 다운로드 타임아웃 (초) — 악성 slow-loris 방지 + 정상 업로드 여유 보장.
_DOWNLOAD_TIMEOUT: float = float(os.getenv("OCR_DOWNLOAD_TIMEOUT", 15.0))

# 도메인 화이트리스트 — 쉼표 구분. 미설정 시 IP 대역 차단만 적용하여
# 로컬 개발 편의를 유지한다. 운영 환경은 반드시 업로드 도메인만 지정할 것.
#   예: OCR_ALLOWED_HOSTS="cdn.monglepick.com,monglepick-uploads.s3.amazonaws.com"
_ALLOWED_HOSTS: frozenset[str] = frozenset(
    h.strip().lower() for h in os.getenv("OCR_ALLOWED_HOSTS", "").split(",") if h.strip()
)

# PaddleOCR 전역 싱글턴 초기화 보호용 락 — 다중 threadpool 워커가
# 동시에 get_ocr_engine() 을 호출할 때 이중 로딩을 방지한다.
_ocr_engine: Optional[PaddleOCR] = None
_ocr_engine_lock = threading.Lock()

# 짧은 변 기준 최소 해상도 (px) — 이 미만이면 업스케일
_MIN_DIM = 800


# ──────────────────────────────────────────────
# PaddleOCR 초기화
# ──────────────────────────────────────────────

def get_ocr_engine() -> PaddleOCR:
    """PaddleOCR 엔진 싱글턴을 반환한다.

    FastAPI 는 sync 호출을 스레드풀에서 실행하므로, 콜드 스타트 시점에
    동시 요청 2건이 들어오면 이중 초기화(모델 로딩 5~10초)가 발생한다.
    `_ocr_engine_lock` 으로 double-checked locking 을 적용해 이를 방지한다.
    """
    global _ocr_engine
    if _ocr_engine is not None:
        return _ocr_engine
    with _ocr_engine_lock:
        if _ocr_engine is not None:
            return _ocr_engine
        logger.info("PaddleOCR 모델 초기화 중 (korean, PP-OCRv5)...")
        # PaddleOCR 3.x 에서는 paddlex 로거가 verbose 출력 — 억제
        logging.getLogger("ppocr").setLevel(logging.WARNING)
        logging.getLogger("paddlex").setLevel(logging.WARNING)
        # 3.x API: 방향/언와핑 모델 비활성화 + 모바일 감지 모델(CPU 최적화)
        _ocr_engine = PaddleOCR(
            lang="korean",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            # text_detection_model_name 미지정: lang="korean" 기본 감지 모델 사용
            # PP-OCRv5_mobile_det 는 한글 텍스트 감지율이 낮아 제거
        )
        logger.info("PaddleOCR 모델 초기화 완료")
        return _ocr_engine


# ──────────────────────────────────────────────
# 공통 전처리 유틸
# ──────────────────────────────────────────────

def _resize_to_target(img: Image.Image, min_dim: int = _MIN_DIM) -> Image.Image:
    """짧은 변이 min_dim 미만이면 비율 유지 업스케일 (LANCZOS)."""
    w, h = img.size
    short = min(w, h)
    if short < min_dim:
        scale = min_dim / short
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    return img


def _crop_receipt_center(img: Image.Image, margin: float = 0.03) -> Image.Image:
    """주변 여백(margin %) 크롭 — 배경 노이즈 제거."""
    w, h = img.size
    return img.crop((
        int(w * margin), int(h * margin),
        int(w * (1 - margin)), int(h * (1 - margin)),
    ))


def _to_gray(img: Image.Image) -> Image.Image:
    return img.convert("RGB").convert("L")


def _denoise_median(gray: Image.Image, size: int = 3) -> Image.Image:
    return gray.filter(ImageFilter.MedianFilter(size=size))


def _to_png_bytes(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ──────────────────────────────────────────────
# 전처리 — gray 단일 변형
# ──────────────────────────────────────────────
# 초기 설계에선 binary/adaptive/sharp/inverted/deskewed 등 6개 변형을 비교했으나,
# 단일 요청 처리 시간이 120초 timeout 에 근접해 운영 안정성 문제가 있었고,
# 실측 결과 gray 변형이 가장 안정적인 점수를 기록했다. gray 단일 변형만
# 유지하여 첫 요청도 여유 있게 완료하도록 정리했다 (2026-04-23 리팩토링).

def _preprocess_variant_gray(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _to_gray(img)
    gray = _denoise_median(gray)
    gray = ImageEnhance.Contrast(gray).enhance(2.0)
    gray = ImageEnhance.Brightness(gray).enhance(1.1)
    gray = gray.filter(ImageFilter.SHARPEN)
    return _to_png_bytes(gray)


# ──────────────────────────────────────────────
# OCR 품질 점수
# ──────────────────────────────────────────────

_SCORE_PATTERNS: List[Tuple[str, float]] = [
    (r"(?:CGV|메가박스|MEGABOX|롯데\s*시네마|LOTTE\s*CINEMA|B[O0]X\s*KIOSK)", 30.0),
    (r"(?:전체|12세|15세|18세|청소년).{0,4}관람",                               25.0),
    (r"(?:일반|성인|청소년|우대|군인)\s*\d+\s*[명매]",                           20.0),
    (r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",                                        20.0),
    (r"\d{2}[./-]\d{1,2}[./-]\d{1,2}",                                        15.0),
    (r"\d{1,2}:\d{2}",                                                         10.0),
    (r"[A-Z가-힣]\s*열\s*\d+\s*번?",                                           15.0),
    (r"\b[A-Z]\d{1,3}\b",                                                      10.0),
    (r"\d+\s*관",                                                               10.0),
    (r"(?:영화|관람|상영|티켓|입장|좌석)",                                        8.0),
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
# PaddleOCR 결과 → 줄 단위 텍스트 재구성
# ──────────────────────────────────────────────

def _build_lines_from_paddleocr(result) -> str:
    """
    PaddleOCR 3.x predict() 결과를 y축 기준으로 묶어 줄 단위 텍스트로 재구성.

    result: list of OCRResult (predict() 반환값)
    각 OCRResult: rec_texts / rec_scores / rec_polys (4-point polygon)
    """
    if not result:
        return ""

    r = result[0]
    texts  = r["rec_texts"]
    scores = r["rec_scores"]
    polys  = r["rec_polys"]

    if not texts:
        return ""

    items = []
    for text, score, poly in zip(texts, scores, polys):
        if not text or score < 0.3:
            continue
        # poly: numpy array or list of [x, y] points
        pts = poly.tolist() if hasattr(poly, "tolist") else poly
        y_coords = [p[1] for p in pts]
        x_coords = [p[0] for p in pts]
        items.append((min(x_coords), sum(y_coords) / len(y_coords), text.strip(), score))

    if not items:
        return ""

    items.sort(key=lambda x: (x[1], x[0]))

    lines: List[str] = []
    current_line: List[Tuple[float, str]] = []
    current_y: Optional[float] = None
    y_tolerance = 18

    for x, y, text, conf in items:
        if current_y is None:
            current_line = [(x, text)]
            current_y = y
            continue
        if abs(y - current_y) <= y_tolerance:
            current_line.append((x, text))
            current_y = (current_y + y) / 2
        else:
            current_line.sort(key=lambda v: v[0])
            lines.append(" ".join(t for _, t in current_line))
            current_line = [(x, text)]
            current_y = y

    if current_line:
        current_line.sort(key=lambda v: v[0])
        lines.append(" ".join(t for _, t in current_line))

    return "\n".join(lines)


# ──────────────────────────────────────────────
# 메인 OCR 실행 — gray 단일 변형
# ──────────────────────────────────────────────

def _ocr_with_best_variant(image_bytes: bytes) -> Tuple[Optional[str], List[str]]:
    """
    gray 전처리 후 PaddleOCR을 실행하고 텍스트를 반환한다.

    Returns:
        best_text  — OCR 텍스트 (실패 시 None)
        all_texts  — 변형 텍스트 리스트 [단일 항목] (폴백 추출용)
    """
    ocr = get_ocr_engine()

    # gray 단일 변형: 재시작 후 첫 요청도 Java 120s timeout 안에 완료
    # 필드 추출 폴백이 필요한 경우 all_texts 에 동일 텍스트가 1개 들어간다
    variants: List[Tuple[str, bytes]] = [
        ("gray", _preprocess_variant_gray(image_bytes)),
    ]

    best_text: Optional[str] = None
    best_score = -1.0
    best_name: Optional[str] = None
    all_texts: List[str] = []
    variant_results: List[Tuple[str, float, str]] = []

    for variant_name, processed in variants:
        try:
            # PaddleOCR 3.x: predict() 에 RGB numpy array 전달
            img_pil = Image.open(io.BytesIO(processed))
            if img_pil.mode != "RGB":
                img_pil = img_pil.convert("RGB")
            img_array = np.array(img_pil)

            result = list(ocr.predict(img_array))
            line_text = _build_lines_from_paddleocr(result)
            all_texts.append(line_text)

            score = _ocr_score(line_text)
            variant_results.append((variant_name, score, line_text))

            if score > best_score:
                best_score = score
                best_text = line_text
                best_name = variant_name

        except Exception as e:
            logger.warning("OCR variant 실패 variant=%s error=%s", variant_name, e)
            all_texts.append("")
            variant_results.append((variant_name, 0.0, ""))

    _log_variant_comparison(variant_results, best_name)
    return best_text, all_texts


def _log_variant_comparison(
    results: List[Tuple[str, float, str]],
    best_name: Optional[str],
) -> None:
    logger.info("── OCR 변형 비교 ──────────────────────────")
    for name, score, text in sorted(results, key=lambda x: x[1], reverse=True):
        marker = "★" if name == best_name else " "
        preview = text[:120].replace("\n", " | ")
        logger.info(
            "%s %-10s score=%6.1f  chars=%4d  preview=%s",
            marker, name, score, len(text), preview,
        )
    logger.info("────────────────────────────────────────────")
    logger.info(
        "선택된 OCR variant=%s score=%.1f",
        best_name,
        max((s for _, s, _ in results), default=0.0),
    )


# ──────────────────────────────────────────────
# 보안 — SSRF 방어 + 스트리밍 크기 제한
# ──────────────────────────────────────────────

class UnsafeImageUrlError(ValueError):
    """SSRF 방어에 의해 거부된 URL 입력 오류."""


def _validate_image_url(image_url: str) -> str:
    """SSRF 방어: scheme/host/IP 대역 검증 후 정제된 URL 을 반환한다.

    거부 기준:
      1. scheme 가 http/https 이외
      2. 호스트명이 비어있거나 해석 실패
      3. 해석된 IP 가 사설/루프백/링크로컬/예약/멀티캐스트 대역
      4. `OCR_ALLOWED_HOSTS` 설정 시 허용 목록에 없는 호스트

    참고: DNS TOCTOU 완화를 위해 운영에서는 반드시 도메인 화이트리스트를
    설정해 이 함수가 IP 레벨 검증만이 아니라 호스트 일치도 요구하게 한다.
    """
    parsed = urlparse(image_url)

    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        raise UnsafeImageUrlError(f"허용되지 않는 스킴: {parsed.scheme!r}")

    host = (parsed.hostname or "").lower()
    if not host:
        raise UnsafeImageUrlError("호스트명이 비어 있습니다")

    # 화이트리스트가 설정된 경우 호스트명 일치 여부를 먼저 검증한다.
    if _ALLOWED_HOSTS and host not in _ALLOWED_HOSTS:
        raise UnsafeImageUrlError(f"허용되지 않은 호스트: {host!r}")

    # 호스트명 → IP 해석 후 사설/예약 대역 차단 (A/AAAA 레코드 모두 검사).
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise UnsafeImageUrlError(f"호스트 해석 실패: {host!r} ({e})") from e

    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            # 해석 결과가 IP 형태가 아닐 수 없으나, 방어적으로 스킵.
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
    """크기 제한을 강제하며 이미지 바이트를 스트리밍 다운로드한다.

    - Content-Length 가 선언된 경우 선검증으로 커넥션을 즉시 끊는다.
    - 선언이 없거나 신뢰할 수 없으면 청크 누적 중 임계치 도달 시 중단한다.
    - 반환값 None 은 다운로드 실패 또는 크기 초과를 의미한다.
    """
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
                        # Content-Length 가 숫자가 아닌 비정상 헤더면 무시하고 스트림 검증으로 대체.
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
    """
    이미지 URL에서 텍스트를 추출한다.

    SSRF/DoS 방어:
      1. `_validate_image_url` 로 스킴·호스트·IP 대역 검증
      2. `_download_image_bytes` 로 스트리밍 크기 제한 적용

    Returns:
        best_text         — OCR 텍스트 (실패·거부 시 None)
        all_variant_texts — 변형 텍스트 리스트 (필드 폴백 추출용)
    """
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
