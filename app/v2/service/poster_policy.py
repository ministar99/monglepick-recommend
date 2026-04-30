"""
검색/추천 응답용 포스터 노출 정책 헬퍼.

규칙:
- `poster_path` 는 TMDB path fragment(`/abc.jpg`) 형태만 허용한다.
- `http://...`, `https://...` 같은 외부 링크는 검색/연관/개인화 추천에서 노출하지 않는다.
- 대표적으로 `file.koreafilm.or.kr` 같은 외부 원본 링크는 전부 무효로 본다.
"""

from __future__ import annotations

INVALID_POSTER_URL_PREFIXES = ("http://", "https://")
ALLOWED_POSTER_HOST_SUBSTRINGS = ("image.tmdb.org",)
INVALID_POSTER_HOST_SUBSTRINGS = ("file.koreafilm.or.kr",)


def normalize_poster_reference(value: object) -> str | None:
    """포스터 경로/URL 값을 공백 제거한 문자열로 정규화합니다."""
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def is_external_poster_reference(value: object) -> bool:
    """외부 URL 형식 포스터인지 판정합니다."""
    normalized = normalize_poster_reference(value)
    if normalized is None:
        return False

    lowered = normalized.lower()
    if any(host in lowered for host in INVALID_POSTER_HOST_SUBSTRINGS):
        return True
    if lowered.startswith(INVALID_POSTER_URL_PREFIXES):
        if any(host in lowered for host in ALLOWED_POSTER_HOST_SUBSTRINGS):
            return False
        return True
    return False


def is_valid_internal_poster_path(value: object) -> bool:
    """TMDB 내부 path fragment 형태의 유효 포스터 경로인지 판정합니다."""
    normalized = normalize_poster_reference(value)
    if normalized is None:
        return False
    if is_external_poster_reference(normalized):
        return False
    return normalized.startswith("/")


def build_tmdb_poster_url(base_url: str, poster_path: object) -> str | None:
    """유효한 내부 포스터 경로만 TMDB 완성 URL로 변환합니다."""
    normalized = normalize_poster_reference(poster_path)
    if normalized is None or not is_valid_internal_poster_path(normalized):
        return None
    return f"{base_url}{normalized}"


def is_allowed_poster_url(value: object) -> bool:
    """
    최종 응답용 poster_url 허용 여부를 판정합니다.

    - TMDB 완성 URL은 허용
    - 외부 원본 링크는 차단
    """
    normalized = normalize_poster_reference(value)
    if normalized is None:
        return False
    if is_valid_internal_poster_path(normalized):
        return True
    lowered = normalized.lower()
    return lowered.startswith(INVALID_POSTER_URL_PREFIXES) and any(
        host in lowered for host in ALLOWED_POSTER_HOST_SUBSTRINGS
    )


def collect_exact_title_candidates(*titles: str | None) -> list[str]:
    """제목 exact match fallback에 사용할 후보 제목 목록을 순서 유지로 수집합니다."""
    collected: list[str] = []
    seen: set[str] = set()
    for raw_title in titles:
        normalized = raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else None
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        collected.append(normalized)
    return collected
