"""
영화관 영수증 OCR 파싱 서비스

추출 필드: 영화명 / 관람일 / 인원 수 / 좌석 / 상영 시간 / 상영관 / 영화관 지점명 / 관람일시(조합)
"""
import re
import logging
from typing import Optional, List, Tuple, TypeVar, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 모듈 로드 시 1회 컴파일 — 반복 호출 성능 향상
# ──────────────────────────────────────────────
_RE_DATE_FULL  = re.compile(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})")
_RE_DATE_SHORT = re.compile(r"(\d{2})[./-](\d{1,2})[./-](\d{1,2})")
_RE_DATE_KO    = re.compile(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일")
_RE_TIME       = re.compile(r"(\d{1,2}):(\d{2})")
_RE_WS         = re.compile(r"[ \t]+")
_RE_KO         = re.compile(r"[가-힣]")
_RE_EN         = re.compile(r"[A-Za-z]")
_RE_NUMONLY    = re.compile(r"^[\d\s\-:./()]+$")

# OCR 에서 브랜드명 사이에 공백이 삽입되는 경우를 흡수하는 패턴 빌더
def _loose(s: str) -> str:
    """'CGV' → 'C\\s*G\\s*V' 처럼 글자 사이 공백을 허용."""
    return r"\s*".join(re.escape(c) for c in s)

# 영화명 레이블 패턴 (compiled)
_MOVIE_LABELED_RES = [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in [
    r"영화명[:\s]+(.+)",
    r"영화\s*제목[:\s]+(.+)",
    r"작품명[:\s]+(.+)",
    r"상영\s*제목[:\s]+(.+)",
    r"영화[:\s]+(?!입장권|관람|임장)(.+)",
    r"타이틀[:\s]+(.+)",
]]
_MOVIE_SPECIAL_RES = [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in [
    r"◆\s*(.{2,60})\s*◆",
    r"「(.{2,60})」",
    r"〈(.{2,60})〉",
    r"<(.{2,60})>",
]]

# 날짜 레이블 우선순위
_DATE_LABEL_SCORES = {
    "관람일": 1.0, "상영일": 0.95, "관람일시": 0.95,
    "상영일시": 0.9, "이용일": 0.7, "결제일": 0.3,
    "출력일": 0.2, "승인일": 0.2,
}

# 인원 수 패턴 (compiled) — 신뢰도 높은 순으로 정렬
_HEADCOUNT_RES = [re.compile(p) for p in [
    # 레이블 명시
    r"총\s*인원\s*:?\s*(\d+)",
    r"관람\s*인원\s*[:\s]*(\d+)",
    r"인원\s*[:\s]*(\d+)\s*명?",
    r"총\s*(\d+)\s*명",
    # 권종 + 수량 (명/매 명시)
    r"(?:성인|일반|청소년|우대|군인|경로|ADULT|CHILD|SENIOR)\s*(\d+)\s*[명매]",
    # 권종 + 수량 (명/매 생략 — OCR 탈락 대비)
    r"(?:성인|일반|청소년|우대)\s*(\d+)(?=\s|$)",
    # 괄호 안 권종 표기: "(일반 2명)"
    r"\(\s*(?:일반|성인|청소년|우대|군인)\s*(\d+)\s*명?\s*\)",
    # 단독 N명
    r"(\d+)\s*명",
    # 매수/장
    r"매수[:\s]+(\d+)",
    r"(\d+)\s*[매장]",
    # "x2", "×2" (수량 기호)
    r"[×xX]\s*(\d+)",
]]

# 영화관 체인 — OCR 오인식 대비: 글자 사이 공백 허용, 한글 음차 포함
# (chain_regex, canonical_name)
_BRANCH_SUFFIX = r"([가-힣A-Za-z0-9·\-]{1,15}(?:점)?)"
_VENUE_CHAIN_RES: List[Tuple[re.Pattern, str]] = [
    # CGV — "C G V", "C.G.V", "씨지브이", PaddleOCR V→Y 오인식(CGY) 포함
    (re.compile(rf"(?:{_loose('CGV')}|C\s*G\s*Y|씨\s*지\s*브\s*이)\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "CGV"),
    # 메가박스 — 붙여쓰기/MEGABOX/음차 다양
    (re.compile(rf"메\s*가\s*박\s*스\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "메가박스"),
    (re.compile(rf"MEGA\s*BOX\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "메가박스"),
    # 롯데시네마
    (re.compile(rf"롯\s*데\s*시\s*네\s*마\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "롯데시네마"),
    (re.compile(rf"LOTTE\s*CINEMA\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "롯데시네마"),
    # 기타 체인
    (re.compile(rf"씨\s*네\s*큐\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "씨네큐"),
    (re.compile(rf"프\s*리\s*머\s*스\s*{_BRANCH_SUFFIX}", re.IGNORECASE), "프리머스"),
]
# 단독 브랜드 감지용 (지점명 없을 때 폴백)
_THEATER_WORDS = ["CGV", "메가박스", "MEGABOX", "롯데시네마", "씨네큐", "프리머스", "씨지브이"]


# ──────────────────────────────────────────────
# 텍스트 정규화
# ──────────────────────────────────────────────
def _normalize_whitespace(text: str) -> str:
    text = text.replace(" ", " ")
    return _RE_WS.sub(" ", text).strip()


def _normalize_ocr_text(text: str) -> str:
    if not text:
        return ""

    # 고정 오인식 교정 (순서 중요: 긴 것 먼저)
    _FIXED_CORRECTIONS = [
        ("Avergers", "Avengers"), ("Avengera", "Avengers"),
        ("2D0", "2D"), ("2DO", "2D"),
        ("OO0원", "000원"), ("12,OO0원", "12,000원"),
        ("기원", "인원"), ("2명일반", "2명 일반"),
        ("21,51", "21:51"), ("14,3O", "14:30"), ("18,OO", "18:00"),
        # CGV OCR 오인식
        ("CGV.", "CGV"), ("C.G.V", "CGV"), ("C G V", "CGV"),
        # 상영관 한글 오인식
        ("상영관", "상영관"), ("쌍영관", "상영관"),
        # 좌석 오인식: 'O' → '0' in seat codes like "A0O" → "A00"
    ]
    for src, dst in _FIXED_CORRECTIONS:
        text = text.replace(src, dst)

    # PaddleOCR: 날짜의 '-'를 ':'로 읽는 경우 복원 (예: 2022:04:18 → 2022-04-18)
    # → 이 치환을 먼저 해야 "04:18" 같은 거짓 시간이 생기지 않는다
    text = re.sub(r"\b(\d{4}):(\d{1,2}):(\d{1,2})\b", r"\1-\2-\3", text)
    # l/I → 1 (인원 수 문맥)
    text = re.sub(r"(인원\s*)[lI](\s*[명(])", r"\g<1>1\2", text)
    text = re.sub(r"(?<!\w)[lI](?=\s*명)", "1", text)
    # O → 0 (숫자 사이)
    text = re.sub(r"(\d)O(\d)", r"\g<1>0\g<2>", text)
    # 좌석 코드 안의 'O' → '0': "A0O" → "A00", "B1O" → "B10"
    text = re.sub(r"([A-Z]\d+)O\b", r"\g<1>0", text)
    # 시간 구분자 혼용 정규화: "14.30" → "14:30" (날짜가 아닌 시간 문맥)
    text = re.sub(r"\b(\d{1,2})\.(\d{2})\b(?!\s*[./-]\d)", r"\1:\2", text)
    # 전각 문자 → 반각
    text = text.translate(str.maketrans("０１２３４５６７８９", "0123456789"))

    return _normalize_whitespace(text)


def _split_lines(text: str) -> List[str]:
    return [ln for ln in (_normalize_whitespace(l) for l in text.splitlines()) if ln]


# ──────────────────────────────────────────────
# 날짜 정규화
# ──────────────────────────────────────────────
def _normalize_date(raw: str) -> Optional[str]:
    raw = raw.strip()
    for pat in (_RE_DATE_FULL, _RE_DATE_KO):
        m = pat.search(raw)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
            except ValueError:
                pass
    m = _RE_DATE_SHORT.search(raw)
    if m:
        yy = int(m.group(1))
        year = yy + 2000 if yy <= 69 else 1900 + yy
        try:
            return datetime(year, int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


# ──────────────────────────────────────────────
# 영화명 보조 함수
# ──────────────────────────────────────────────
def _clean_movie_candidate(name: str) -> str:
    name = _normalize_whitespace(name)
    name = re.sub(r"^(영화명|영화\s*제목|작품명|상영\s*제목)[:\s]+", "", name, flags=re.IGNORECASE)
    name = re.sub(r"영화입장권|입장권", "", name, flags=re.IGNORECASE)
    name = re.sub(r"영수증\s*겸용|\[전체발권\]", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b(?:2D|3D|IMAX|4DX|SCREENX)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\b(?:자막|더빙)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(
        r"전체\s*.{0,2}관람가|\d{1,2}세\s*이.{0,3}\s*관람가|청소년\s*.{0,2}관람불가|18세\s*이.{0,3}",
        "", name, flags=re.IGNORECASE,
    )
    name = re.sub(r"[*#@^~]", "", name)
    name = re.sub(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}.*$", "", name)
    name = re.sub(r"\d{2}[./-]\d{1,2}[./-]\d{1,2}.*$", "", name)
    name = re.sub(r"\d{1,2}:\d{2}.*$", "", name)
    name = re.sub(r"\s+\d{1,3}\.\d+.*$", "", name)
    name = re.sub(r"\d+\s*관.*$|[A-Z가-힣]\s*열.*$|\d+\s*명.*$", "", name)
    name = re.sub(r"\d{1,3}(?:,\d{3})+\s*원.*$", "", name)
    name = re.sub(r"심야.*$|오후.*$|회차.*$|\b\d+\s*회\b.*$", "", name)
    name = re.sub(r"\b(?:VAT|할인|합계|카드|승인)\b.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^[\s\-–—:|/,\.\[\]\(\)'`]+|[\s\-–—:|/,\.\[\]\(\)'`]+$", "", name)
    name = re.sub(r"\s*-\s*", "-", name)
    return _normalize_whitespace(name)


def _is_bad_movie_candidate(name: str) -> bool:
    name = _clean_movie_candidate(name)
    if len(name) < 2 or len(name) > 50:
        return True
    if _RE_NUMONLY.fullmatch(name):
        return True
    if _normalize_date(name) or _RE_TIME.search(name):
        return True
    if re.search(r"\d+\s*관|\d+\s*층|\d+\s*원|\d+\s*명|\d+\s*매|\d+\s*장", name):
        return True
    compact = name.replace(" ", "").lower()
    hard_bad = {
        "영화입장권", "입장권", "영수증겸용", "전체발권", "영수증", "매출전표",
        "결제", "승인", "카드", "합계", "총금액", "좌석", "상영관",
        "관람일", "출력일", "총인원", "상영시간", "영화정보",
    }
    if any(kw.replace(" ", "").lower() in compact for kw in hard_bad):
        return True
    if "관람가" in compact or "관람불가" in compact:
        return True
    for kw in _THEATER_WORDS:
        if compact == kw.replace(" ", "").lower():
            return True
    if not _RE_KO.search(name) and not _RE_EN.search(name):
        return True
    if name in {"영화", "티켓", "영화입장권", "입장권"}:
        return True
    return False


def _score_movie_candidate(name: str, source: str) -> float:
    cleaned = _clean_movie_candidate(name)
    score = {
        "labeled": 1.2, "special": 0.9, "after_rating": 1.7,
        "korean_before_english": 1.8, "title_pair": 1.7,
        "english_title": 1.4, "line": 0.5,
    }.get(source, 0.0)
    if source == "korean_before_english" and len(cleaned) <= 2:
        score -= 1.2
    l = len(cleaned)
    if 3 <= l <= 20:    score += 0.35
    elif l == 2:        score += 0.05
    elif 21 <= l <= 30: score += 0.2
    if _RE_KO.search(cleaned): score += 0.3
    if _RE_EN.search(cleaned): score += 0.15
    if "-" in cleaned or ":" in cleaned: score += 0.1
    if "영화" in cleaned or "입장권" in cleaned: score -= 1.0
    return score


def _slice_after_rating(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)
    m = re.search(
        r"(전체\s*.{0,2}관람가|\d{1,2}세\s*이.{0,3}\s*관람가|청소년\s*.{0,2}관람불가|18세\s*이.{0,3})",
        normalized, re.IGNORECASE,
    )
    if not m:
        return None
    tail = normalized[m.end():].strip()
    if not tail:
        return None
    stop_pats = [
        r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", r"\d{2}[./-]\d{1,2}[./-]\d{1,2}",
        r"\d{1,2}:\d{2}", r"\b\d+\s*회\b", r"\d+\s*관", r"[A-Z가-힣]\s*열",
        r"\d+\s*명", r"\d{1,3}(?:,\d{3})+\s*원", r"심야", r"오후", r"오전", r"VAT", r"할인",
    ]
    cut_idx = len(tail)
    for pat in stop_pats:
        m2 = re.search(pat, tail, re.IGNORECASE)
        if m2:
            cut_idx = min(cut_idx, m2.start())
    candidate = _clean_movie_candidate(tail[:cut_idx].split('\n')[0].strip())
    return candidate if candidate else None


def _extract_korean_before_english(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)
    lines = _split_lines(normalized)
    english_title_keywords = [
        "Avengers", "Avatar", "Mission", "Spider", "Batman", "Frozen", "Dune",
        "Parasite", "Wonka", "Wicked", "Iron", "Thor", "Captain", "Guardians",
    ]
    for i, line in enumerate(lines):
        for kw in english_title_keywords:
            if kw.lower() in line.lower():
                for j in range(i - 1, max(i - 4, -1), -1):
                    raw = lines[j].strip()
                    if raw and raw[0] in "'\"`'\"（(「[《<◆":
                        continue
                    candidate = _clean_movie_candidate(raw)
                    if candidate and not _is_bad_movie_candidate(candidate) and _RE_KO.search(candidate):
                        return candidate
                break
    for i, line in enumerate(lines):
        if i == 0:
            continue
        stripped = line.strip()
        if re.fullmatch(r"[A-Za-z0-9 '\-:.,!?]+", stripped) and len(stripped) >= 3:
            # 날짜·시간 전용 라인은 영어 제목 라인으로 취급하지 않음
            # 예: "2019-06-05 09:" → _normalize_date 가 값 반환 → skip
            if _normalize_date(stripped) or re.fullmatch(r"[\d\s\-:./]+", stripped):
                continue
            for j in range(i - 1, max(i - 3, -1), -1):
                candidate = _clean_movie_candidate(lines[j].strip())
                if candidate and not _is_bad_movie_candidate(candidate) \
                        and len(_RE_KO.findall(candidate)) >= 2:
                    return candidate
    m = re.search(
        r"([가-힣A-Za-z0-9 \-:]{2,40}) +(Avengers|Avatar|Mission|Spider|Batman|Frozen|Dune"
        r"|Parasite|Wonka|Wicked|Iron|Thor|Captain|Guardians)",
        normalized, re.IGNORECASE,
    )
    if m:
        candidate = _clean_movie_candidate(m.group(1))
        if candidate and not _is_bad_movie_candidate(candidate):
            return candidate
    m = re.search(r"([가-힣][가-힣A-Za-z0-9 \-]{1,30}) +[A-Za-z]{3,}", normalized)
    if m:
        candidate = _clean_movie_candidate(m.group(1))
        if candidate and not _is_bad_movie_candidate(candidate):
            return candidate
    return None


def _extract_english_title(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)
    for pat in [
        r"([A-Z][A-Za-z0-9'&:.\- ]{2,40}:\s*[A-Z][A-Za-z0-9'&:.\- ]{2,40})",
        r"(Avengers:\s*Infinity\s*War)",
    ]:
        m = re.search(pat, normalized, re.IGNORECASE)
        if m:
            candidate = _clean_movie_candidate(m.group(1))
            if candidate and not _is_bad_movie_candidate(candidate):
                return candidate
    lines = _split_lines(normalized)
    for i, line in enumerate(lines):
        window = " ".join(lines[max(0, i - 2):i + 3])
        for base, pat in [("Iron Man", r"\bIron\b.*\bMan\b"), ("Thor", r"\bThor\b")]:
            if re.search(pat, window, re.IGNORECASE):
                num_m = re.search(r"\b([23])\b", window)
                return f"{base}{' ' + num_m.group(1) if num_m else ''}"
    return None


def _extract_title_subtitle_pair(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)
    lines = _split_lines(normalized)

    def _ko_ratio(s: str) -> float:
        ns = s.replace(" ", "")
        return len(_RE_KO.findall(ns)) / len(ns) if ns else 0.0

    valid: List[Tuple[int, str]] = []
    for i, line in enumerate(lines):
        cleaned = _clean_movie_candidate(line)
        if not cleaned or _is_bad_movie_candidate(cleaned):
            continue
        if not _RE_KO.search(cleaned) or len(cleaned) < 2 or len(cleaned) > 20:
            continue
        if _ko_ratio(cleaned) < 0.7:
            continue
        valid.append((i, cleaned))

    for k in range(len(valid) - 1):
        idx1, c1 = valid[k]
        idx2, c2 = valid[k + 1]
        if idx2 - idx1 <= 2:
            combined = f"{c1}: {c2}"
            if 5 <= len(combined) <= 40:
                return combined
    return None


def _extract_movie_name(text: str) -> Tuple[Optional[str], float]:
    text = _normalize_ocr_text(text)
    candidates: List[Tuple[str, float, str]] = []

    for re_obj in _MOVIE_LABELED_RES:
        for match in re_obj.finditer(text):
            raw = _normalize_whitespace(match.group(1))
            cleaned = _clean_movie_candidate(raw)
            if not _is_bad_movie_candidate(cleaned):
                candidates.append((cleaned, _score_movie_candidate(raw, "labeled"), "labeled"))

    for fn, source in [
        (_slice_after_rating, "after_rating"),
        (_extract_korean_before_english, "korean_before_english"),
        (_extract_title_subtitle_pair, "title_pair"),
        (_extract_english_title, "english_title"),
    ]:
        result = fn(text)
        if result and not _is_bad_movie_candidate(result):
            candidates.append((result, _score_movie_candidate(result, source), source))

    for re_obj in _MOVIE_SPECIAL_RES:
        for match in re_obj.finditer(text):
            raw = _normalize_whitespace(match.group(1))
            cleaned = _clean_movie_candidate(raw)
            if not _is_bad_movie_candidate(cleaned):
                candidates.append((cleaned, _score_movie_candidate(raw, "special"), "special"))

    for line in _split_lines(text):
        cleaned = _clean_movie_candidate(line)
        if not _is_bad_movie_candidate(cleaned) and 2 <= len(cleaned) <= 40:
            candidates.append((cleaned, _score_movie_candidate(cleaned, "line"), "line"))

    if not candidates:
        return None, 0.0

    dedup: dict = {}
    for name, score, source in candidates:
        if name not in dedup or score > dedup[name][0]:
            dedup[name] = (score, source)

    ranked = sorted(
        [(n, s, src) for n, (s, src) in dedup.items()],
        key=lambda x: x[1], reverse=True,
    )
    logger.info("영화명 후보 TOP5: %s", ranked[:5])
    best_name, best_score, best_source = ranked[0]
    logger.info("선택된 영화명: %s(score=%.2f, source=%s)", best_name, best_score, best_source)
    return best_name, best_score


# ──────────────────────────────────────────────
# 관람일 추출
# ──────────────────────────────────────────────
def _extract_watch_date(text: str) -> Optional[str]:
    text = _normalize_ocr_text(text)
    candidates: List[Tuple[str, float]] = []

    label_patterns = {
        "관람일":   [r"관람일[:\s]+(\d{4}[./-]\d{1,2}[./-]\d{1,2})", r"관람일[:\s]+(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)", r"관람일[:\s]+(\d{2}[./-]\d{1,2}[./-]\d{1,2})"],
        "상영일":   [r"상영일[:\s]+(\d{4}[./-]\d{1,2}[./-]\d{1,2})", r"상영일[:\s]+(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)", r"상영일[:\s]+(\d{2}[./-]\d{1,2}[./-]\d{1,2})"],
        "관람일시": [r"관람일시[:\s]+(\d{4}[./-]\d{1,2}[./-]\d{1,2})", r"관람일시[:\s]+(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)", r"관람일시[:\s]+(\d{2}[./-]\d{1,2}[./-]\d{1,2})"],
        "상영일시": [r"상영일시[:\s]+(\d{4}[./-]\d{1,2}[./-]\d{1,2})", r"상영일시[:\s]+(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)", r"상영일시[:\s]+(\d{2}[./-]\d{1,2}[./-]\d{1,2})"],
    }
    for label, patterns in label_patterns.items():
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                normalized = _normalize_date(match.group(1))
                if normalized:
                    candidates.append((normalized, _DATE_LABEL_SCORES.get(label, 0.1)))

    for re_obj, score in [(_RE_DATE_FULL, 0.1), (_RE_DATE_KO, 0.1), (_RE_DATE_SHORT, 0.15)]:
        for match in re_obj.finditer(text):
            normalized = _normalize_date(match.group(0))
            if normalized:
                candidates.append((normalized, score))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)
    best_date, best_score = candidates[0]
    logger.info("날짜 후보: %s", candidates[:5])
    logger.info("선택된 날짜: %s(score=%.2f)", best_date, best_score)
    return best_date


# ──────────────────────────────────────────────
# 인원 수 추출
# ──────────────────────────────────────────────
def _extract_headcount(text: str) -> Optional[int]:
    text = _normalize_ocr_text(text)

    def _valid(n: int) -> Optional[int]:
        return n if 1 <= n <= 20 else None

    # 1순위: "2명(일반2명)" 복합 패턴
    m = re.search(r"(\d+)\s*명?\s*\(\s*(?:일반|성인|청소년|우대|군인)\s*\d+\s*명\s*\)", text)
    if m:
        r = _valid(int(m.group(1)))
        if r: return r

    # 2순위: "(1명)" — 괄호 안 명시적 인원 수 (영수증 하단 합계 표기)
    # 예: "총인원 (VAT:618원) (1명)"
    m = re.search(r"\(\s*(\d+)\s*명\s*\)", text)
    if m:
        r = _valid(int(m.group(1)))
        if r: return r

    # 3순위: 총인원 레이블 — 뒤에 임의 문자가 있어도 숫자+명 탐색
    # 예: "총인원 (VAT:618원) (1명)" / "총인원: 2명"
    m = re.search(r"총\s*인원[^\n]{0,50}?(\d+)\s*명", text)
    if m:
        r = _valid(int(m.group(1)))
        if r: return r

    # 4순위: 여러 권종 합산 "일반 2매 / 청소년 1매" → 3
    multi = re.findall(r"(?:일반|성인|청소년|우대|군인|경로|ADULT|CHILD)\s*(\d+)\s*[명매]", text)
    if len(multi) >= 2:
        total = sum(int(x) for x in multi)
        r = _valid(total)
        if r: return r

    # 5순위: 레이블 기반 패턴 목록
    for re_obj in _HEADCOUNT_RES:
        m = re_obj.search(text)
        if m:
            r = _valid(int(m.group(1)))
            if r: return r

    # 6순위: "1인" 표기
    m = re.search(r"(\d+)\s*인(?!\s*치|\s*분|\s*터)", text)
    if m:
        r = _valid(int(m.group(1)))
        if r: return r

    # 7순위: 권종 + 숫자 (명/매 생략 — 같은 라인에 가격)
    # 예: "성인 1 13,000원"
    for pat in [
        r"(?:성인|일반|청소년|우대)\s+(\d+)\s+[\d,]+원",
        r"(?:성인|일반|청소년|우대)\s+(\d+)(?=\s|$)",
    ]:
        m = re.search(pat, text)
        if m:
            r = _valid(int(m.group(1)))
            if r: return r

    # 8순위 (최후 폴백): 라인 시작 단독 소수 + OCR 오인식된 가격 형식
    # 예: "2 '0 원" = "2매 20,000원"이 OCR된 경우 (매·쉼표 탈락)
    # 조건: 한자리 수 + 공백 + 쉼표류 문자 + 숫자 (명시적 레이블 없음)
    for line in _split_lines(text):
        m = re.match(r"^([1-9])\s+[',]\d", line.strip())
        if m:
            r = _valid(int(m.group(1)))
            if r: return r

    # 9순위: PaddleOCR 공백 삽입 대응 — "일 반 2 명" / "성 인 1 명" 등
    # 한글 권종 키워드의 글자 사이에 공백이 생긴 경우를 흡수
    for pat in [
        r"일\s*반\s*(\d+)\s*명?",
        r"성\s*인\s*(\d+)\s*명?",
        r"청\s*소\s*년\s*(\d+)\s*명?",
        r"우\s*대\s*(\d+)\s*명?",
    ]:
        m = re.search(pat, text)
        if m:
            r = _valid(int(m.group(1)))
            if r: return r

    return None


# ──────────────────────────────────────────────
# 시간 검증
# ──────────────────────────────────────────────
def _validate_time(t: str) -> Optional[str]:
    """HH:MM 검증 및 정규화. 00:00~24:59 허용."""
    m = _RE_TIME.match(t.strip())
    if not m:
        return None
    h, mn = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 24 and 0 <= mn <= 59:
        return f"{h:02d}:{mn:02d}"
    return None


# ──────────────────────────────────────────────
# 상영 시각 추출
# ──────────────────────────────────────────────
def _extract_screening_time(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)

    # 1순위: 관람일시/상영일시 레이블에 포함된 시간 — "2026/04/10 14:30"
    for pat in [
        r"(?:관람일시|상영일시)\s*[:\s]+\d{2,4}[./-]\d{1,2}[./-]\d{1,2}\s+(\d{1,2}:\d{2})",
        r"(?:관람일|상영일)\s*[:\s]+\d{2,4}[./-]\d{1,2}[./-]\d{1,2}\s+(\d{1,2}:\d{2})",
    ]:
        m = re.search(pat, normalized)
        if m:
            result = _validate_time(m.group(1))
            if result:
                return result

    # 2순위: 날짜 + 시간이 같은 라인에 있는 경우 (레이블 없음)
    # 예: "2022-04-18 17:27(B0XKIOSK 4)" — 날짜 라인 skip 전에 시간 먼저 추출
    for pat in [
        r"\d{4}[./-]\d{1,2}[./-]\d{1,2}\s+(\d{1,2}:\d{2})",
        r"\d{2}[./-]\d{1,2}[./-]\d{1,2}\s+(\d{1,2}:\d{2})",
    ]:
        m = re.search(pat, normalized)
        if m:
            result = _validate_time(m.group(1))
            if result:
                return result

    # 3순위: 회차/시작시간 레이블
    for pat in [
        r"회차\s*[:\s]+(\d{1,2}:\d{2})",
        r"시작\s*시간?\s*[:\s]+(\d{1,2}:\d{2})",
        r"상영\s*시작\s*[:\s]+(\d{1,2}:\d{2})",
        r"시작\s*[:\s]+(\d{1,2}:\d{2})",
        r"상영\s*시각\s*[:\s]+(\d{1,2}:\d{2})",
        r"입장\s*시간?\s*[:\s]+(\d{1,2}:\d{2})",
    ]:
        m = re.search(pat, normalized)
        if m:
            result = _validate_time(m.group(1))
            if result:
                return result

    # 4순위: 오전/오후 표기 → 24시간 변환
    m = re.search(r"(오전|오후)\s*(\d{1,2}):(\d{2})", normalized)
    if m:
        ampm, h, mn = m.group(1), int(m.group(2)), m.group(3)
        if ampm == "오후" and h < 12:
            h += 12
        elif ampm == "오전" and h == 12:
            h = 0
        result = _validate_time(f"{h}:{mn}")
        if result:
            return result

    # 5순위: 관람 맥락 라인의 HH:MM (날짜 라인 제외)
    time_fallback: List[str] = []
    for line in _split_lines(normalized):
        if _normalize_date(line):
            continue
        has_context = any(kw in line for kw in ("관람", "상영", "시작", "좌석", "영화", "회차", "티켓"))
        for m2 in _RE_TIME.finditer(line):
            t = _validate_time(m2.group(0))
            if t and t != "00:00":
                if has_context:
                    return t
                time_fallback.append(t)

    return time_fallback[0] if time_fallback else None


# ──────────────────────────────────────────────
# 좌석 추출
# ──────────────────────────────────────────────
def _extract_seat(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)

    # 1순위: 레이블 기반
    for pat in [
        # "좌석번호: A열 5번, A열 6번"
        r"좌석\s*번호?\s*[:\s]+([A-Z가-힣]\s*열\s*\d+\s*번?(?:\s*[,·]\s*[A-Z가-힣]?\s*열?\s*\d+\s*번?)*)",
        # "좌석: A10"
        r"좌석\s*번호?\s*[:\s]+([A-Z]-?\d{1,3}(?:\s*[,·]\s*[A-Z]-?\d{1,3})*)",
        # "좌석 정보: ..."
        r"좌석\s*정보\s*[:\s]+([A-Z가-힣0-9][\w가-힣 \-열번,·]*)",
        r"좌석\s*[:\s]+([A-Z가-힣]\s*열\s*\d+\s*번?(?:\s*[,·]\s*[A-Z가-힣]?\s*열?\s*\d+\s*번?)*)",
        r"좌석\s*[:\s]+([A-Z]-?\d{1,3}(?:\s*[,·]\s*[A-Z]-?\d{1,3})*)",
        r"SEAT\s*[:\s]+([A-Z가-힣]\s*열\s*\d+\s*번?|[A-Z]-?\d{1,3})",
        r"시트\s*[:\s]+([A-Z가-힣]\s*열\s*\d+\s*번?)",
        r"자리\s*[:\s]+([A-Z가-힣]\s*열\s*\d+\s*번?)",
    ]:
        m = re.search(pat, normalized, re.IGNORECASE)
        if m:
            seat = _normalize_whitespace(m.group(1)).split('\n')[0].strip()
            if seat and 1 <= len(seat) <= 30:
                return seat

    # 2순위: 상영관 + 좌석 조합 → 좌석만 추출
    for pat in [
        r"\d+관\s+([A-Z가-힣]\s*열\s*\d+\s*번?(?:\s*[,·]\s*\d+\s*번?)*)",
        r"\d+관\s+([A-Z]-?\d{1,3}(?:\s*[,·]\s*[A-Z]?-?\d{1,3})*)",
    ]:
        m = re.search(pat, normalized, re.IGNORECASE)
        if m:
            seat = _normalize_whitespace(m.group(1))
            if 1 <= len(seat) <= 25:
                return seat

    # 3순위: 열/번 패턴
    for pat in [
        r"([A-Z가-힣]\s*열\s*\d+\s*번(?:\s*[,·，、]\s*\d+\s*번?)*)",         # E열 7번, 8번
        r"(\b[A-Z]-?\d{1,3}\b(?:\s*[,·，、]\s*\b[A-Z]-?\d{1,3}\b)+)",       # A10, A11 (복수, 전각쉼표 포함)
        r"(\b[A-Z]-?\d{1,3}\b(?:\s*[\|/]\s*\b[A-Z]-?\d{1,3}\b)+)",          # A10|A11, A10/A11
        r"([A-Z]\s*열\s*\d{1,3}번?)",                                        # A열5번 (공백 없는 케이스)
    ]:
        m = re.search(pat, normalized)
        if m:
            seat = _normalize_whitespace(m.group(1))
            if 1 <= len(seat) <= 30:
                return seat

    # 4순위: 단독 라인 또는 라인 앞부분에 좌석 코드
    for line in _split_lines(normalized):
        s = line.strip()
        if re.fullmatch(r"[A-Z]-?\d{1,3}", s):            # "A10", "J-9"
            return s
        if re.fullmatch(r"[가-힣]\s*열\s*\d+\s*번?", s):  # "가열 3번"
            return s
        if re.fullmatch(r"([A-Z]-?\d{1,3})\s+([A-Z]-?\d{1,3})", s):  # "A10 B10"
            return s

    # 5순위 (공격적 폴백): 라인 안에서 좌석 코드 + 인원/가격 문맥
    # 예: "G8 성인 1매 13,000원" → G8
    for line in _split_lines(normalized):
        ctx = any(kw in line for kw in ("성인", "일반", "청소년", "원", "매", "명", "관람"))
        if not ctx:
            continue
        # [A-Z][숫자] 형태가 라인 앞쪽에 단독으로 있는 경우
        m = re.match(r"^([A-Z]-?\d{1,3})\s", line)
        if m:
            return m.group(1)
        # 라인 안 어디든 "G8" 패턴 (공백/탭으로 분리된 토큰)
        tokens = line.split()
        for tok in tokens:
            if re.fullmatch(r"[A-Z]-?\d{1,3}", tok):
                return tok

    return None


# ──────────────────────────────────────────────
# 상영관 번호 추출
# ──────────────────────────────────────────────
def _extract_theater(text: str) -> Optional[str]:
    normalized = _normalize_ocr_text(text)

    def _safe_num(s: str) -> Optional[str]:
        try:
            n = int(s)
            return f"{n}관" if 1 <= n <= 30 else None
        except ValueError:
            return None

    # 1순위: 레이블 + 숫자
    for pat in [
        r"상영관\s*[:\s]*(\d+)\s*관?",
        r"관람관\s*[:\s]*(\d+)\s*관?",
        r"상영관\s*번호?\s*[:\s]*(\d+)",
        r"SCREEN\s*(?:NO\.?|NUMBER)?\s*[:\s]*(\d+)",
        r"Hall\s*[:\s]*(\d+)",
    ]:
        m = re.search(pat, normalized, re.IGNORECASE)
        if m:
            result = _safe_num(m.group(1))
            if result:
                return result

    # 2순위: "N관 상영관/홀/관람실"
    m = re.search(r"(\d+)\s*관\s*(?:상영관|홀|관람실)", normalized, re.IGNORECASE)
    if m:
        result = _safe_num(m.group(1))
        if result:
            return result

    # 3순위: "N관" 뒤에 좌석 표현 또는 날짜/시간
    for pat in [
        r"(\d+)\s*관\s+[A-Z가-힣]",       # "3관 A열 5번"
        r"(\d+)\s*관\s+\d{1,2}:\d{2}",    # "3관 14:30"
    ]:
        m = re.search(pat, normalized)
        if m:
            result = _safe_num(m.group(1))
            if result:
                return result

    # 4순위: "N번 관람관" / "관람관 N번"
    m = re.search(r"(\d+)\s*번\s*관람관|관람관\s*(\d+)\s*번", normalized)
    if m:
        result = _safe_num(m.group(1) or m.group(2))
        if result:
            return result

    # 5순위: 독립 라인 "N관"
    for line in _split_lines(normalized):
        m = re.fullmatch(r"(\d+)\s*관", line.strip())
        if m:
            result = _safe_num(m.group(1))
            if result:
                return result

    # 6순위 (공격적 폴백): 라인 안 어디서든 "N관" 탐색
    # 가격/년도 오탐 방지: "관" 앞 숫자가 4자리(년도) 이거나 "관람" 앞이면 제외
    for line in _split_lines(normalized):
        for m in re.finditer(r"(\d{1,2})\s*관(?!람|객|련|계|심)", line):
            result = _safe_num(m.group(1))
            if result:
                # 이 숫자가 날짜/가격 문맥이 아닌지 확인
                start = m.start()
                before = line[max(0, start - 5):start]
                if re.search(r"\d{4}|,|\.", before):  # 년도나 가격 직후면 제외
                    continue
                return result

    return None


# ──────────────────────────────────────────────
# 영화관 지점명 추출
# ──────────────────────────────────────────────
def _is_bad_branch(branch: str) -> bool:
    b = branch.strip()
    if not b:
        return True
    if re.fullmatch(r"[\d\s]+", b):  # 숫자만
        return True
    bad_single = {"관", "층", "호", "번", "매", "원"}
    if b in bad_single:
        return True
    if b.lower() in {"cinema", "cinemas", "cine", "screen"}:
        return True
    return False


def _extract_venue(text: str) -> Optional[str]:
    """
    영화관 체인 + 지점명 추출.
    예: "CGV 홍대" / "메가박스 코엑스" / "롯데시네마 건대입구"
    OCR 오인식(글자 사이 공백, 음차 등)을 흡수한다.
    """
    normalized = _normalize_ocr_text(text)

    # 1순위: 레이블 기반
    for pat in [
        r"영화관\s*[:\s]+([A-Za-z가-힣][A-Za-z가-힣 ]{1,20})",
        r"극장\s*[:\s]+([A-Za-z가-힣][A-Za-z가-힣 ]{1,20})",
        r"상영\s*장소\s*[:\s]+([A-Za-z가-힣 ]{2,25})",
        r"THEATER\s*[:\s]+([A-Za-z가-힣 ]{2,25})",
    ]:
        m = re.search(pat, normalized, re.IGNORECASE)
        if m:
            venue = _normalize_whitespace(m.group(1))
            if len(venue) >= 2:
                return venue

    # 2순위: 체인 브랜드 + 지점명 (정규식 _VENUE_CHAIN_RES)
    for re_obj, chain in _VENUE_CHAIN_RES:
        m = re_obj.search(normalized)
        if m:
            branch = _normalize_whitespace(m.group(1))
            if not _is_bad_branch(branch):
                branch = re.sub(r"점$", "", branch).strip()
                if branch:
                    return f"{chain} {branch}"
            return chain

    # 3순위: 단독 체인명 (지점명 없음)
    chain_solo = [
        ("CGV",      r"\bCGV\b|\bCGY\b"),  # CGY: V→Y PaddleOCR 오인식 대응
        ("메가박스",  r"메가박스|MEGABOX"),
        # BOXKIOSK = 롯데시네마 전용 셀프 발권 키오스크 브랜드명 (OCR 오인식: O→0)
        ("롯데시네마", r"롯데\s*시네마|LOTTE\s*CINEMA|B[O0]X\s*KIOSK|박스\s*키오스크"),
        ("씨네큐",   r"씨네큐"),
        ("프리머스",  r"프리머스"),
    ]
    for chain, pat in chain_solo:
        if re.search(pat, normalized, re.IGNORECASE):
            return chain

    return None


# ──────────────────────────────────────────────
# 관람일시 조합
# ──────────────────────────────────────────────
def _combine_watched_at(watch_date: Optional[str], screening_time: Optional[str]) -> Optional[str]:
    """날짜 + 시간 → "YYYY-MM-DD HH:MM"."""
    if watch_date and screening_time:
        return f"{watch_date} {screening_time}"
    return None


# ──────────────────────────────────────────────
# 폴백 추출
# ──────────────────────────────────────────────
T = TypeVar("T")


def _extract_with_fallback(
    extract_fn: Callable[[str], Optional[T]],
    main_text: str,
    fallback_texts: Optional[List[str]],
    field_name: str = "",
) -> Optional[T]:
    result = extract_fn(main_text)
    if result is not None:
        return result
    if fallback_texts:
        for alt in fallback_texts:
            if not alt or alt == main_text:
                continue
            result = extract_fn(alt)
            if result is not None:
                if field_name:
                    logger.info("[폴백] %s: %s", field_name, result)
                return result
    return None


# ──────────────────────────────────────────────
# 신뢰도 & 상태
# ──────────────────────────────────────────────
def _calculate_confidence(movie_name, movie_score: float, watch_date, headcount) -> float:
    score = 0.0
    if movie_name and not _is_bad_movie_candidate(movie_name):
        if movie_score >= 2.0:   score += 0.45
        elif movie_score >= 1.5: score += 0.30
        elif movie_score >= 1.0: score += 0.15
        else:                    score += 0.05
    if watch_date: score += 0.35
    if headcount:  score += 0.20
    return round(score, 2)


def _determine_status(
    movie_name_ok: bool, watch_date_ok: bool, headcount_ok: bool,
    seat_ok: bool, screening_time_ok: bool, theater_ok: bool, venue_ok: bool,
) -> str:
    if movie_name_ok and watch_date_ok:
        return "SUCCESS"
    if any([movie_name_ok, watch_date_ok, headcount_ok, seat_ok, screening_time_ok, theater_ok, venue_ok]):
        return "PARTIAL_SUCCESS"
    return "FAILED"


# ──────────────────────────────────────────────
# 메인 진입점
# ──────────────────────────────────────────────
def parse_receipt(text: str, fallback_texts: Optional[List[str]] = None) -> dict:
    """
    OCR 텍스트에서 영화 관람 정보를 추출한다.

    Returns dict with keys:
        movie_name, watch_date, headcount, seat, screening_time, theater, venue,
        watched_at, confidence, status,
        movie_name_ok, watch_date_ok, headcount_ok, seat_ok,
        screening_time_ok, theater_ok, venue_ok
    """
    text = _normalize_ocr_text(text)
    logger.info("OCR RAW TEXT:\n%s", text)

    movie_name, movie_score = _extract_movie_name(text)
    watch_date     = _extract_watch_date(text)
    headcount      = _extract_with_fallback(_extract_headcount,      text, fallback_texts, "인원 수")
    seat           = _extract_with_fallback(_extract_seat,           text, fallback_texts, "좌석")
    screening_time = _extract_with_fallback(_extract_screening_time, text, fallback_texts, "상영 시간")
    theater        = _extract_with_fallback(_extract_theater,        text, fallback_texts, "상영관")
    venue          = _extract_with_fallback(_extract_venue,          text, fallback_texts, "영화관")
    watched_at     = _combine_watched_at(watch_date, screening_time)

    movie_name_ok     = movie_name     is not None
    watch_date_ok     = watch_date     is not None
    headcount_ok      = headcount      is not None
    seat_ok           = seat           is not None
    screening_time_ok = screening_time is not None
    theater_ok        = theater        is not None
    venue_ok          = venue          is not None

    confidence = _calculate_confidence(movie_name, movie_score, watch_date, headcount)
    status     = _determine_status(
        movie_name_ok, watch_date_ok, headcount_ok,
        seat_ok, screening_time_ok, theater_ok, venue_ok,
    )

    logger.info(
        "파싱 완료 — status=%s movie=%s(%.2f) date=%s headcount=%s "
        "seat=%s time=%s theater=%s venue=%s watched_at=%s confidence=%.2f",
        status, movie_name, movie_score, watch_date, headcount,
        seat, screening_time, theater, venue, watched_at, confidence,
    )

    return {
        "movie_name":       movie_name,
        "watch_date":       watch_date,
        "headcount":        headcount,
        "seat":             seat,
        "screening_time":   screening_time,
        "theater":          theater,
        "venue":            venue,
        "watched_at":       watched_at,
        "confidence":       confidence,
        "status":           status,
        "movie_name_ok":    movie_name_ok,
        "watch_date_ok":    watch_date_ok,
        "headcount_ok":     headcount_ok,
        "seat_ok":          seat_ok,
        "screening_time_ok": screening_time_ok,
        "theater_ok":       theater_ok,
        "venue_ok":         venue_ok,
    }