"""
Pydantic DTO 모델 — Raw SQL 결과(dict) → 파이썬 객체 매핑

SQLAlchemy ORM 엔티티(v1 entity.py)를 대체합니다.
aiomysql DictCursor가 반환하는 딕셔너리를 Pydantic 모델로 변환합니다.

JSON 컬럼(genres, cast 등)은 MySQL에서 문자열로 반환되므로,
validator에서 json.loads()로 파싱합니다.

DDL 기준: Backend JPA 엔티티 (ddl-auto=update, 진실 원본)
"""

import json
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, field_validator


class MovieDTO(BaseModel):
    """
    영화 DTO (읽기 전용)

    movies 테이블의 DictCursor 결과를 매핑합니다.
    PK: movie_id VARCHAR(50) — TMDB/KOBIS/KMDb 등 다양한 소스의 ID가 공존합니다.
    """
    movie_id: str
    title: str
    title_en: Optional[str] = None
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    release_year: Optional[int] = None
    runtime: Optional[int] = None
    rating: Optional[float] = None
    vote_count: Optional[int] = None
    popularity_score: Optional[float] = None
    genres: Any = None        # JSON 컬럼 — 문자열 또는 리스트
    director: Optional[str] = None
    cast: Any = None           # JSON 컬럼 — 문자열 또는 리스트
    certification: Optional[str] = None
    trailer_url: Optional[str] = None
    overview: Optional[str] = None
    tagline: Optional[str] = None
    imdb_id: Optional[str] = None
    original_language: Optional[str] = None
    collection_name: Optional[str] = None
    # KOBIS 보강 컬럼
    kobis_movie_cd: Optional[str] = None
    sales_acc: Optional[int] = None
    audience_count: Optional[int] = None
    screen_count: Optional[int] = None
    kobis_watch_grade: Optional[str] = None
    kobis_open_dt: Optional[str] = None
    # KMDb 보강 컬럼
    kmdb_id: Optional[str] = None
    awards: Optional[str] = None
    filming_location: Optional[str] = None
    # 데이터 출처
    source: Optional[str] = None

    class Config:
        # DictCursor 결과(dict)에서 직접 생성 허용
        from_attributes = True

    def get_genres_list(self) -> list[str]:
        """JSON 장르를 파이썬 리스트로 변환합니다."""
        if not self.genres:
            return []
        # aiomysql은 JSON 컬럼을 문자열로 반환
        if isinstance(self.genres, list):
            return self.genres
        if isinstance(self.genres, str):
            try:
                parsed = json.loads(self.genres)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, TypeError):
                return []
        return []

    def get_cast_list(self) -> list[str]:
        """JSON 배우 목록을 파이썬 리스트로 변환합니다."""
        if not self.cast:
            return []
        if isinstance(self.cast, list):
            return self.cast
        if isinstance(self.cast, str):
            try:
                parsed = json.loads(self.cast)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, TypeError):
                return []
        return []


class UserPreferenceDTO(BaseModel):
    """
    사용자 선호도 DTO (읽기/쓰기)

    user_preferences 테이블의 DictCursor 결과를 매핑합니다.
    JSON 컬럼(preferred_genres, preferred_moods 등)은 문자열→리스트 변환합니다.
    """
    id: int
    user_id: str
    preferred_genres: Any = None
    preferred_moods: Any = None
    preferred_directors: Any = None
    preferred_actors: Any = None
    preferred_eras: Any = None
    excluded_genres: Any = None
    preferred_platforms: Any = None
    preferred_certification: Optional[str] = None
    extra_preferences: Any = None

    class Config:
        from_attributes = True

    def get_genres_list(self) -> list[str]:
        """preferred_genres JSON을 리스트로 변환합니다."""
        return _parse_json_list(self.preferred_genres)

    def get_moods_list(self) -> list[str]:
        """preferred_moods JSON을 리스트로 변환합니다."""
        return _parse_json_list(self.preferred_moods)


class SearchHistoryDTO(BaseModel):
    """
    검색 이력 DTO

    search_history 테이블의 DictCursor 결과를 매핑합니다.
    """
    id: int
    user_id: str
    keyword: str
    searched_at: datetime

    class Config:
        from_attributes = True


class TrendingKeywordDTO(BaseModel):
    """
    인기 검색어 DTO

    trending_keywords 테이블의 DictCursor 결과를 매핑합니다.
    """
    id: int
    keyword: str
    search_count: int
    last_searched_at: datetime

    class Config:
        from_attributes = True


class LikeDTO(BaseModel):
    """
    영화 좋아요 DTO (Raw SQL 결과 매핑).

    likes 테이블의 DictCursor 결과를 매핑한다.
    Backend JPA 엔티티(monglepick-backend Like.java)와 1:1 대응되며,
    DDL은 Backend가 마스터(ddl-auto=update)이므로 이 DTO는 읽기/쓰기용이지만
    스키마 변경 권한은 없다.

    <h3>소프트 삭제 정책</h3>
    - deleted_at IS NULL → 활성 좋아요
    - deleted_at IS NOT NULL → 취소된 좋아요 (복구 가능)

    <h3>BaseAuditEntity 공통 컬럼</h3>
    created_at, updated_at, created_by, updated_by는 Backend JPA가 자동 관리하지만
    Raw SQL에서 INSERT 시 명시해 주거나 DB default를 활용해야 한다.
    """
    # 좋아요 레코드 PK (BIGINT AUTO_INCREMENT)
    like_id: int
    # 사용자 ID (VARCHAR 50)
    user_id: str
    # 영화 ID (VARCHAR 50)
    movie_id: str
    # 소프트 삭제 시각 (null이면 활성)
    deleted_at: Optional[datetime] = None
    # BaseAuditEntity 자동 컬럼
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None

    class Config:
        from_attributes = True

    def is_active(self) -> bool:
        """활성 좋아요 여부 판정 (deleted_at IS NULL)."""
        return self.deleted_at is None


class WorldcupResultDTO(BaseModel):
    """
    이상형 월드컵 결과 DTO

    worldcup_results 테이블의 DictCursor 결과를 매핑합니다.
    JSON 컬럼(semi_final_movie_ids, selection_log, genre_preferences)은 문자열로 저장됩니다.
    """
    id: int
    user_id: str
    round_size: int
    winner_movie_id: str
    runner_up_movie_id: Optional[str] = None
    semi_final_movie_ids: Optional[str] = None   # JSON 문자열
    selection_log: Optional[str] = None           # JSON 문자열
    genre_preferences: Optional[str] = None       # JSON 문자열
    onboarding_completed: bool = False
    created_at: datetime

    class Config:
        from_attributes = True


# ─────────────────────────────────────────
# 유틸리티 함수
# ─────────────────────────────────────────

def _parse_json_list(value: Any) -> list[str]:
    """JSON 컬럼 값을 파이썬 리스트로 변환합니다."""
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []
