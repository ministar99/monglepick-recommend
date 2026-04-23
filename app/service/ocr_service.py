"""
영화관 영수증 OCR 서비스

이미지 전처리(gray 단일 변형) → PaddleOCR → 텍스트 추출
gray 전처리: 회색조 + 중간 대비 + 노이즈 제거 + 샤픈
"""
import io
import os
import re
import logging
from typing import Optional, List, Tuple

# PaddleOCR 시작 시 모델 서버 연결 체크를 건너뜀 (시작 속도 향상)
os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

import numpy as np
import httpx
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
from paddleocr import PaddleOCR

logger = logging.getLogger(__name__)

_ocr_engine: Optional[PaddleOCR] = None

# 짧은 변 기준 최소 해상도 (px) — 이 미만이면 업스케일
_MIN_DIM = 800


# ──────────────────────────────────────────────
# PaddleOCR 초기화
# ──────────────────────────────────────────────

def get_ocr_engine() -> PaddleOCR:
    global _ocr_engine
    if _ocr_engine is None:
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


def _denoise_gaussian(gray: Image.Image, radius: float = 0.8) -> Image.Image:
    return gray.filter(ImageFilter.GaussianBlur(radius=radius))


def _adaptive_threshold(gray: Image.Image, block_size: int = 31, c: int = 8) -> Image.Image:
    """
    OpenCV adaptiveThreshold 대응 — numpy + PIL 구현.
    조명이 불균일한 영수증에서 글자 검출률을 높인다.
    """
    radius = block_size / 6.0
    arr = np.array(gray, dtype=np.float32)
    blurred = np.array(
        gray.filter(ImageFilter.GaussianBlur(radius=radius)), dtype=np.float32
    )
    binary = np.where(arr >= blurred - c, 255, 0).astype(np.uint8)
    result = Image.fromarray(binary)
    return result.filter(ImageFilter.MaxFilter(3))


def _deskew(img: Image.Image, angle_range: int = 5) -> Tuple[Image.Image, float]:
    """
    투영 프로파일 분산 최대화 방식 기울기 보정.
    ±angle_range 도 범위를 0.5도 단위로 탐색.
    """
    gray_small = _to_gray(img).resize(
        (img.width // 2, img.height // 2), Image.LANCZOS
    )
    arr = np.array(gray_small, dtype=np.float32)
    mean_val = float(np.mean(arr))
    binary = (arr < mean_val).astype(np.float32)

    best_angle = 0.0
    best_var = -1.0
    angles = [a * 0.5 for a in range(-angle_range * 2, angle_range * 2 + 1)]

    for angle in angles:
        rotated = Image.fromarray((binary * 255).astype(np.uint8)).rotate(
            angle, expand=False, fillcolor=0
        )
        proj = np.sum(np.array(rotated), axis=1)
        var = float(np.var(proj))
        if var > best_var:
            best_var = var
            best_angle = angle

    if abs(best_angle) < 0.5:
        return img, 0.0

    corrected = img.rotate(
        best_angle, expand=False,
        fillcolor=(255, 255, 255) if img.mode == "RGB" else 255,
    )
    return corrected, best_angle


def _to_png_bytes(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ──────────────────────────────────────────────
# 전처리 변형 6종
# ──────────────────────────────────────────────

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


def _preprocess_variant_binary(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _to_gray(img)
    gray = _denoise_median(gray)
    gray = ImageEnhance.Contrast(gray).enhance(2.5)
    bw = gray.point(lambda x: 255 if x > 160 else 0, mode="1").convert("L")
    return _to_png_bytes(bw)


def _preprocess_variant_adaptive(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _to_gray(img)
    gray = _denoise_gaussian(gray, radius=0.8)
    adaptive = _adaptive_threshold(gray, block_size=31, c=8)
    return _to_png_bytes(adaptive)


def _preprocess_variant_sharp(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _to_gray(img)
    gray = ImageEnhance.Contrast(gray).enhance(1.8)
    gray = gray.filter(ImageFilter.UnsharpMask(radius=2.0, percent=200, threshold=3))
    gray = gray.filter(ImageFilter.SHARPEN)
    return _to_png_bytes(gray)


def _preprocess_variant_inverted(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    gray = _to_gray(img)
    inverted = ImageOps.invert(gray)
    inverted = _denoise_median(inverted)
    inverted = ImageEnhance.Contrast(inverted).enhance(2.2)
    inverted = inverted.filter(ImageFilter.SHARPEN)
    return _to_png_bytes(inverted)


def _preprocess_variant_deskewed(image_bytes: bytes) -> Tuple[bytes, float]:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = _crop_receipt_center(img)
    img = _resize_to_target(img)
    corrected, angle = _deskew(img)
    gray = _to_gray(corrected)
    gray = _denoise_median(gray)
    gray = ImageEnhance.Contrast(gray).enhance(2.0)
    gray = gray.filter(ImageFilter.SHARPEN)
    return _to_png_bytes(gray), angle


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
# 메인 OCR 실행 — 6개 변형 비교
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
# 공개 API
# ──────────────────────────────────────────────

def re_search_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(p, text) for p in patterns)


async def extract_text_from_url(image_url: str) -> Tuple[Optional[str], List[str]]:
    """
    이미지 URL에서 텍스트를 추출한다.

    Returns:
        best_text         — 최고 품질 변형의 텍스트 (실패 시 None)
        all_variant_texts — 모든 변형 텍스트 (필드 폴백 추출용)
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(image_url)
            response.raise_for_status()
            image_bytes = response.content

        best_text, all_texts = _ocr_with_best_variant(image_bytes)

        if not best_text:
            logger.warning("OCR 추출 결과 없음")
            return None, []

        logger.info("OCR 추출 완료 — 글자 수: %d", len(best_text))
        return best_text, all_texts

    except httpx.HTTPError as e:
        logger.error("이미지 다운로드 실패 url=%s error=%s", image_url, e)
        return None, []
    except Exception as e:
        logger.error("OCR 처리 오류: %s", e)
        return None, []
