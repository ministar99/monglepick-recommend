"""
인기 검색어 오버레이 조합 유틸리티

기본 trending_keywords 순위 위에 popular_search_keyword 운영 메타를 겹쳐
최종 사용자 노출 리스트를 계산합니다.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TrendingOverlayCandidate:
    """자동 집계 기반 인기 검색어 후보"""
    keyword: str
    search_count: int
    base_rank: int


@dataclass(frozen=True)
class PopularSearchOverlayMeta:
    """관리자 인기 검색어 운영 메타"""
    keyword: str
    display_rank: int | None = None
    manual_priority: int = 0
    is_excluded: bool = False


@dataclass(frozen=True)
class PopularSearchResultItem:
    """오버레이 적용 후 최종 노출 항목"""
    keyword: str
    search_count: int


@dataclass(frozen=True)
class _MergedCandidate:
    keyword: str
    search_count: int
    base_rank: int
    display_rank: int | None
    manual_priority: int


def build_popular_search_ranking(
    trending_candidates: list[TrendingOverlayCandidate],
    overlay_keywords: list[PopularSearchOverlayMeta],
    limit: int,
) -> list[PopularSearchResultItem]:
    """
    자동 집계 결과와 운영 메타를 병합해 최종 노출 순위를 계산합니다.

    규칙:
    - 기본 후보는 trending_keywords 순위입니다.
    - is_excluded=true 키워드는 제거합니다.
    - display_rank가 있으면 해당 순위에 우선 배치합니다.
    - manual_priority가 높을수록 나머지 후보보다 먼저 노출합니다.
    - 운영 메타에만 존재하는 키워드도 display_rank/manual_priority가 있으면 노출합니다.
    """
    if limit <= 0:
        return []

    overlay_by_keyword: dict[str, PopularSearchOverlayMeta] = {}
    for overlay in overlay_keywords:
        keyword = overlay.keyword.strip()
        if not keyword:
            continue
        overlay_by_keyword[keyword] = PopularSearchOverlayMeta(
            keyword=keyword,
            display_rank=overlay.display_rank,
            manual_priority=overlay.manual_priority or 0,
            is_excluded=bool(overlay.is_excluded),
        )

    merged_by_keyword: dict[str, _MergedCandidate] = {}
    for item in trending_candidates:
        keyword = item.keyword.strip()
        if not keyword or keyword in merged_by_keyword:
            continue

        overlay = overlay_by_keyword.get(keyword)
        if overlay and overlay.is_excluded:
            continue

        merged_by_keyword[keyword] = _MergedCandidate(
            keyword=keyword,
            search_count=max(int(item.search_count), 0),
            base_rank=max(int(item.base_rank), 1),
            display_rank=overlay.display_rank if overlay else None,
            manual_priority=overlay.manual_priority if overlay else 0,
        )

    for overlay in overlay_keywords:
        keyword = overlay.keyword.strip()
        if not keyword or overlay.is_excluded or keyword in merged_by_keyword:
            continue

        should_force_include = overlay.display_rank is not None or (overlay.manual_priority or 0) > 0
        if not should_force_include:
            continue

        merged_by_keyword[keyword] = _MergedCandidate(
            keyword=keyword,
            search_count=0,
            base_rank=10**9,
            display_rank=overlay.display_rank,
            manual_priority=overlay.manual_priority or 0,
        )

    fixed_candidates: list[_MergedCandidate] = []
    floating_candidates: list[_MergedCandidate] = []
    for item in merged_by_keyword.values():
        if item.display_rank is None:
            floating_candidates.append(item)
            continue

        if item.display_rank < 1:
            floating_candidates.append(
                _MergedCandidate(
                    keyword=item.keyword,
                    search_count=item.search_count,
                    base_rank=item.base_rank,
                    display_rank=None,
                    manual_priority=item.manual_priority,
                )
            )
            continue

        if item.display_rank > limit:
            continue

        fixed_candidates.append(item)

    fixed_candidates.sort(
        key=lambda item: (
            item.display_rank,
            -item.manual_priority,
            -item.search_count,
            item.base_rank,
            item.keyword,
        )
    )
    floating_candidates.sort(
        key=lambda item: (
            -item.manual_priority,
            -item.search_count,
            item.base_rank,
            item.keyword,
        )
    )

    slots: list[_MergedCandidate | None] = [None] * limit
    for item in fixed_candidates:
        slot_index = item.display_rank - 1
        while slot_index < limit and slots[slot_index] is not None:
            slot_index += 1
        if slot_index < limit:
            slots[slot_index] = item

    floating_iter = iter(floating_candidates)
    for index, slot in enumerate(slots):
        if slot is not None:
            continue

        next_item = next(floating_iter, None)
        if next_item is None:
            break
        slots[index] = next_item

    results: list[PopularSearchResultItem] = []
    for item in slots:
        if item is None:
            continue
        results.append(
            PopularSearchResultItem(
                keyword=item.keyword,
                search_count=item.search_count,
            )
        )

    return results
