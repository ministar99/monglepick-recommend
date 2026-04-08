"""
검색 페이지 전용 장르 카탈로그

첨부된 `genre_master.csv`를 기준으로 검색 페이지에서 노출할 장르 옵션을 정리합니다.

정제 규칙:
- `contents_count <= 20` 인 장르는 제외
- `코메디`, `에로` 장르는 제외
- 괄호 표기는 사용자 노출 라벨에서 제거 또는 병합
- 유사 장르는 하나의 키워드로 묶고, 실제 검색 시에는 alias 전체를 매칭
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SearchGenreCatalogEntry:
    """검색 페이지 장르 옵션 1건"""

    label: str
    aliases: tuple[str, ...]
    contents_count: int


# CSV 등장 순서를 최대한 유지하면서 병합/제외 규칙을 반영한 최종 검색 장르 목록
SEARCH_GENRE_CATALOG: tuple[SearchGenreCatalogEntry, ...] = (
    SearchGenreCatalogEntry("액션", ("액션",), 54167),
    SearchGenreCatalogEntry("드라마", ("드라마",), 307188),
    SearchGenreCatalogEntry("애니메이션", ("애니메이션",), 74751),
    SearchGenreCatalogEntry("로맨스", ("로맨스",), 69529),
    SearchGenreCatalogEntry("범죄", ("범죄",), 41199),
    SearchGenreCatalogEntry("공포", ("공포", "공포(호러)"), 732364),
    SearchGenreCatalogEntry("스릴러", ("스릴러",), 57819),
    SearchGenreCatalogEntry("코미디", ("코미디",), 166500),
    SearchGenreCatalogEntry("다큐멘터리", ("다큐멘터리",), 212328),
    SearchGenreCatalogEntry("전쟁", ("전쟁",), 12966),
    SearchGenreCatalogEntry("판타지", ("판타지",), 29680),
    SearchGenreCatalogEntry("모험", ("모험", "어드벤처", "활극"), 27876),
    SearchGenreCatalogEntry("가족", ("가족",), 34461),
    SearchGenreCatalogEntry("SF", ("SF",), 25766),
    SearchGenreCatalogEntry("아동", ("아동",), 497),
    SearchGenreCatalogEntry("음악", ("음악", "뮤직"), 61683),
    SearchGenreCatalogEntry("군사", ("군사",), 962),
    SearchGenreCatalogEntry("역사", ("역사",), 21545),
    SearchGenreCatalogEntry("멜로드라마", ("멜로드라마",), 492),
    SearchGenreCatalogEntry("미스터리", ("미스터리",), 25351),
    SearchGenreCatalogEntry("갱스터", ("갱스터",), 34),
    SearchGenreCatalogEntry("스포츠", ("스포츠",), 245),
    SearchGenreCatalogEntry("지역", ("지역",), 512),
    SearchGenreCatalogEntry("자연ㆍ환경", ("자연ㆍ환경",), 472),
    SearchGenreCatalogEntry("문화", ("문화",), 911),
    SearchGenreCatalogEntry("사회", ("사회", "사회물(경향)"), 955),
    SearchGenreCatalogEntry("인물", ("인물",), 1512),
    SearchGenreCatalogEntry("기업ㆍ기관ㆍ단체", ("기업ㆍ기관ㆍ단체",), 142),
    SearchGenreCatalogEntry("첩보", ("첩보",), 67),
    SearchGenreCatalogEntry("TV 영화", ("TV 영화",), 31914),
    SearchGenreCatalogEntry("서부", ("서부", "서부극(웨스턴)"), 9862),
    SearchGenreCatalogEntry("옴니버스", ("옴니버스",), 61),
    SearchGenreCatalogEntry("시대극/사극", ("시대극/사극",), 329),
    SearchGenreCatalogEntry("전기", ("전기",), 95),
    SearchGenreCatalogEntry("반공/분단", ("반공/분단",), 127),
    SearchGenreCatalogEntry("청춘/하이틴", ("청춘영화", "하이틴(고교)"), 148),
    SearchGenreCatalogEntry("계몽", ("계몽",), 698),
    SearchGenreCatalogEntry("문예", ("문예",), 25),
    SearchGenreCatalogEntry("종교", ("종교",), 55),
    SearchGenreCatalogEntry("교육", ("교육",), 339),
    SearchGenreCatalogEntry("재난", ("재난",), 29),
    SearchGenreCatalogEntry("인권", ("인권",), 379),
    SearchGenreCatalogEntry("로드무비", ("로드무비",), 34),
    SearchGenreCatalogEntry("기록", ("기록",), 141),
    SearchGenreCatalogEntry("과학", ("과학",), 192),
    SearchGenreCatalogEntry("느와르", ("느와르",), 55),
    SearchGenreCatalogEntry("예술", ("예술",), 174),
    SearchGenreCatalogEntry("다부작", ("다부작",), 224),
    SearchGenreCatalogEntry("동성애", ("동성애",), 38),
    SearchGenreCatalogEntry("TV드라마", ("TV드라마",), 91),
    SearchGenreCatalogEntry("공연", ("공연",), 305),
)

_SEARCH_GENRE_LABEL_MAP = {
    entry.label: entry
    for entry in SEARCH_GENRE_CATALOG
}


def get_search_genre_options() -> list[SearchGenreCatalogEntry]:
    """검색 페이지에서 노출할 장르 옵션 목록을 반환합니다."""
    return list(SEARCH_GENRE_CATALOG)


def normalize_search_genre_labels(labels: list[str] | None) -> list[str]:
    """
    클라이언트에서 넘어온 장르 라벨 목록을 카탈로그 기준으로 정규화합니다.

    존재하지 않는 라벨은 버리고, 중복은 첫 등장 순서만 유지합니다.
    """

    if not labels:
        return []

    normalized: list[str] = []
    seen: set[str] = set()

    for label in labels:
        cleaned = label.strip()
        if not cleaned or cleaned in seen:
            continue
        if cleaned not in _SEARCH_GENRE_LABEL_MAP:
            continue

        seen.add(cleaned)
        normalized.append(cleaned)

    return normalized


def expand_search_genre_aliases(labels: list[str] | None) -> list[str]:
    """
    장르 라벨 목록을 실제 movies.genres JSON 매칭용 alias 목록으로 확장합니다.

    예: `["모험"]` -> `["모험", "어드벤처", "활극"]`
    """

    aliases: list[str] = []
    seen: set[str] = set()

    for label in normalize_search_genre_labels(labels):
        entry = _SEARCH_GENRE_LABEL_MAP[label]
        for alias in entry.aliases:
            if alias in seen:
                continue

            seen.add(alias)
            aliases.append(alias)

    return aliases
