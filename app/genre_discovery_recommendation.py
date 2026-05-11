"""
장르-only 검색에서 사용하는 추천순 점수 규칙.

키워드가 없는 장르 탐색 검색의 `relevance`는 문자열 관련도가 아니라
"지금 추천하기 좋은 영화" 순서로 해석한다.
"""

from __future__ import annotations

from datetime import date

# 고평점이지만 표본이 적은 영화를 과도하게 올리지 않도록 보수적인 prior 를 둔다.
GENRE_DISCOVERY_PRIOR_RATING_NORM = 0.58
GENRE_DISCOVERY_PRIOR_VOTE_COUNT = 150.0

# popularity_score 는 평점보다 약한 보조 신호로만 사용한다.
GENRE_DISCOVERY_POPULARITY_SCALE = 80.0

# 최신성은 약한 보정만 주고, 30년이 지나면 더 이상 가산하지 않는다.
GENRE_DISCOVERY_FRESHNESS_WINDOW_YEARS = 30.0

# 최종 가중치.
GENRE_DISCOVERY_RATING_WEIGHT = 0.70
GENRE_DISCOVERY_POPULARITY_WEIGHT = 0.10
GENRE_DISCOVERY_FRESHNESS_WEIGHT = 0.20


GENRE_DISCOVERY_RECOMMENDATION_ES_SCRIPT_SOURCE = """
double ratingNorm = doc['rating'].size() == 0 ? 0.0 : doc['rating'].value / 10.0;
double voteCount = doc['vote_count'].size() == 0 ? 0.0 : doc['vote_count'].value;
double bayesianRating =
    ((ratingNorm * voteCount) + (params.prior_rating_norm * params.prior_vote_count)) /
    (voteCount + params.prior_vote_count);

double popularity = doc['popularity_score'].size() == 0 ? 0.0 : doc['popularity_score'].value;
double popularityNorm = popularity <= 0.0 ? 0.0 : popularity / (popularity + params.popularity_scale);

double freshness = 0.0;
if (doc['release_year'].size() > 0) {
    double ageYears = params.current_year - doc['release_year'].value;
    if (ageYears <= 0.0) {
        freshness = 1.0;
    } else if (ageYears < params.freshness_window_years) {
        freshness = (params.freshness_window_years - ageYears) / params.freshness_window_years;
    }
}

return
    (bayesianRating * params.rating_weight) +
    (popularityNorm * params.popularity_weight) +
    (freshness * params.freshness_weight);
"""


def build_genre_discovery_recommendation_params(
    *,
    current_year: int | None = None,
) -> dict[str, float]:
    """장르 추천순 계산에 필요한 공통 파라미터를 반환한다."""
    normalized_current_year = float(current_year or date.today().year)
    return {
        "prior_rating_norm": GENRE_DISCOVERY_PRIOR_RATING_NORM,
        "prior_vote_count": GENRE_DISCOVERY_PRIOR_VOTE_COUNT,
        "popularity_scale": GENRE_DISCOVERY_POPULARITY_SCALE,
        "freshness_window_years": GENRE_DISCOVERY_FRESHNESS_WINDOW_YEARS,
        "rating_weight": GENRE_DISCOVERY_RATING_WEIGHT,
        "popularity_weight": GENRE_DISCOVERY_POPULARITY_WEIGHT,
        "freshness_weight": GENRE_DISCOVERY_FRESHNESS_WEIGHT,
        "current_year": normalized_current_year,
    }
