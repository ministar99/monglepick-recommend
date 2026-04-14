"""
Prometheus 커스텀 메트릭 중앙 정의 모듈 (monglepick-recommend).

prometheus-fastapi-instrumentator 가 기본 HTTP 메트릭을 자동으로 노출하지만,
내부 비즈니스 로직(캐시 hit/miss, 특정 쿼리 지속시간)은 여기에 명시적으로 정의한다.

### 라벨 컨벤션 (카디널리티 가드)
- 라벨에 movie_id, user_id 같은 가변 값을 넣지 않는다.
- endpoint 라벨은 URL 경로가 아닌 논리 이름("match_cowatched" 등)으로 한다.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram


# ============================================================
# Movie Match — Co-watched CF 캐시 & 쿼리 지속시간
# ============================================================

# CF Redis 캐시 조회 결과 카운터.
#   outcome : "hit" | "miss" | "error" (Redis 장애)
match_cowatch_cache_total: Counter = Counter(
    "monglepick_recommend_match_cowatch_cache_total",
    "Movie Match Co-watched CF Redis 캐시 접근 결과",
    labelnames=("outcome",),
)

# CF MySQL 쿼리 지속시간 (캐시 miss 케이스에서만 기록).
# reviews INNER JOIN 의 p95/p99 관찰 → 느린 쿼리 감지용.
match_cowatch_query_duration_seconds: Histogram = Histogram(
    "monglepick_recommend_match_cowatch_query_duration_seconds",
    "Movie Match Co-watched CF MySQL 쿼리 소요 시간 (초)",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 2.0, 3.0, 5.0),
)

# CF 엔드포인트 전체 응답 시간 (캐시 hit/miss 포함 E2E).
# HTTP 미들웨어의 http_request_duration_seconds 와 별도로 논리 엔드포인트 단위 측정.
match_cowatch_endpoint_duration_seconds: Histogram = Histogram(
    "monglepick_recommend_match_cowatch_endpoint_duration_seconds",
    "Movie Match Co-watched CF 엔드포인트 전체 응답 시간 (초, cache hit 포함)",
    labelnames=("cache",),  # "hit" | "miss"
    buckets=(0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 5.0),
)
