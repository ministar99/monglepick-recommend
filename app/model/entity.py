"""
SQLAlchemy ORM 엔티티 모델 정의

DDL 기준: Backend JPA 엔티티 (ddl-auto=update, 진실 원본)
이 서비스는 Backend JPA 스키마에 맞춰 읽기 전용으로 매핑합니다.

공유 테이블 (Backend JPA DDL 기준, 읽기 전용):
- movies: 영화 경량 참조 (PK: movie_id VARCHAR(50))
- users: 사용자 기본 정보 (PK: user_id VARCHAR(50))
- user_preferences: 사용자 취향 프로필 (읽기/쓰기)
- grades: 사용자 등급 마스터 (PK: grade_id BIGINT, 읽기 전용)
  └ 2026-03-31 신규: UserGrade enum → DB 테이블로 전환
- user_points: 사용자 포인트 잔액 (PK: user_point_id BIGINT, 읽기 전용)
  └ 2026-03-31 변경: point_have → balance, user_grade → grade_id FK
- achievement_types: 업적 유형 마스터 (PK: achievement_type_id BIGINT, 읽기 전용)
  └ 2026-03-31 신규: achievement_type VARCHAR → FK 분리

이 서비스가 소유하는 테이블:
- search_history: 사용자별 최근 검색 이력
- trending_keywords: 인기 검색어 집계
- worldcup_session: 이상형 월드컵 진행 세션 저장
- worldcup_match: 이상형 월드컵 라운드별 매치 저장
- worldcup_results: 이상형 월드컵 결과 저장

주의: 비즈니스 키(movie_id/user_id)는 VARCHAR(50)이며,
      서로게이트 PK는 BIGINT AUTO_INCREMENT입니다.
"""

import json
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    func,
)

from app.core.database import Base

# ─────────────────────────────────────────
# SQLite 호환 BigInteger 타입
# ─────────────────────────────────────────
# DDL은 BIGINT AUTO_INCREMENT이지만, SQLite는 INTEGER PRIMARY KEY만 자동 증가 지원.
# with_variant()로 MySQL에서는 BIGINT, SQLite에서는 INTEGER로 매핑.
AutoIncrementBigInt = BigInteger().with_variant(Integer, "sqlite")


# =========================================
# 공유 테이블 (Backend JPA DDL 기준, 읽기 전용 매핑)
# =========================================

class Movie(Base):
    """
    영화 엔티티 (읽기 전용)

    DDL: init.sql의 movies 테이블과 동일한 구조입니다.
    PK는 movie_id VARCHAR(50) — TMDB/KOBIS/KMDb 등 다양한 소스의 ID가 공존합니다.
    이 서비스에서는 검색/조회 목적으로만 사용합니다.
    """
    __tablename__ = "movies"
    # 기존 테이블 유지 (DDL 변경 금지)
    __table_args__ = {"extend_existing": True}

    # ── PK: 영화 고유 식별자 (TMDB ID, KOBIS 코드, KMDb ID 등) ──
    movie_id: str = Column(String(50), primary_key=True, comment="영화 ID (TMDB/KOBIS/KMDb)")
    # 한국어 제목
    title: str = Column(String(500), nullable=False, comment="한국어 제목")
    # 영어 제목
    title_en: str | None = Column(String(500), nullable=True, comment="영어 제목")
    # TMDB 포스터 경로 (예: "/abcdef.jpg")
    poster_path: str | None = Column(String(500), nullable=True, comment="TMDB 포스터 경로")
    # TMDB 배경 이미지 경로
    backdrop_path: str | None = Column(String(500), nullable=True, comment="TMDB 배경 이미지 경로")
    # 개봉 연도 (DDL: release_year INT)
    release_year: int | None = Column(Integer, nullable=True, comment="개봉 연도")
    # 상영 시간 (분)
    runtime: int | None = Column(Integer, nullable=True, comment="상영 시간 (분)")
    # 평균 평점 (0~10)
    rating: float | None = Column(Float, nullable=True, comment="평균 평점 (0~10)")
    # 투표 수
    vote_count: int | None = Column(Integer, nullable=True, comment="투표 수")
    # TMDB 인기도 점수
    popularity_score: float | None = Column(Float, nullable=True, comment="TMDB 인기도 점수")
    # 장르 목록 (JSON 배열: ["액션", "드라마"])
    genres = Column(JSON, nullable=True, comment='장르 목록 ["액션","드라마"]')
    # 감독 이름
    director: str | None = Column(String(200), nullable=True, comment="감독 이름")
    # 주연 배우 목록 (JSON 배열: ["배우1", "배우2"])
    # DB 컬럼명은 cast_members (Backend JPA Entity 기준), Python 속성명은 cast_members로 통일
    cast_members = Column("cast_members", JSON, nullable=True, comment='주연 배우 목록 ["배우1","배우2"]')
    # 관람등급 (전체관람가, 12세 등)
    certification: str | None = Column(String(50), nullable=True, comment="관람등급")
    # YouTube 트레일러 URL
    trailer_url: str | None = Column(String(500), nullable=True, comment="YouTube 트레일러 URL")
    # 줄거리
    overview: str | None = Column(Text, nullable=True, comment="줄거리")
    # 태그라인
    tagline: str | None = Column(String(500), nullable=True, comment="태그라인")
    # IMDb ID (tt로 시작)
    imdb_id: str | None = Column(String(20), nullable=True, comment="IMDb ID")
    # 원본 언어 코드 (en, ko 등)
    original_language: str | None = Column(String(10), nullable=True, comment="원본 언어 코드")
    # 성인물 여부
    adult: bool | None = Column(Boolean, nullable=True, comment="성인물 여부")
    # 프랜차이즈/컬렉션 이름
    collection_name: str | None = Column(String(200), nullable=True, comment="프랜차이즈/컬렉션 이름")
    # ── KOBIS 보강 컬럼 ──
    kobis_movie_cd: str | None = Column(String(20), nullable=True, comment="KOBIS 영화 코드")
    sales_acc: int | None = Column(BigInteger, nullable=True, comment="누적 매출액 (KRW)")
    audience_count: int | None = Column(BigInteger, nullable=True, comment="관객수")
    screen_count: int | None = Column(Integer, nullable=True, comment="최대 상영 스크린 수")
    kobis_watch_grade: str | None = Column(String(50), nullable=True, comment="KOBIS 관람등급")
    kobis_open_dt: str | None = Column(String(10), nullable=True, comment="KOBIS 개봉일 (YYYYMMDD)")
    # ── KMDb 보강 컬럼 ──
    kmdb_id: str | None = Column(String(50), nullable=True, comment="KMDb 영화 ID")
    awards: str | None = Column(Text, nullable=True, comment="수상 내역")
    filming_location: str | None = Column(Text, nullable=True, comment="촬영 장소")
    # ── 데이터 출처 추적 ──
    source: str | None = Column(String(20), nullable=True, comment="데이터 출처 (tmdb/kaggle/kobis/kmdb)")

    def get_genres_list(self) -> list[str]:
        """JSON 장르를 파이썬 리스트로 변환합니다."""
        if not self.genres:
            return []
        # JSON 컬럼은 이미 파이썬 리스트로 디시리얼라이즈될 수 있음
        if isinstance(self.genres, list):
            return self.genres
        try:
            return json.loads(self.genres)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_cast_list(self) -> list[str]:
        """JSON 배우 목록을 파이썬 리스트로 변환합니다."""
        if not self.cast_members:
            return []
        if isinstance(self.cast_members, list):
            return self.cast_members
        try:
            return json.loads(self.cast_members)
        except (json.JSONDecodeError, TypeError):
            return []


class User(Base):
    """
    사용자 엔티티 (읽기 전용)

    DDL: init.sql의 users 테이블과 동일한 구조입니다.
    PK는 user_id VARCHAR(50) — Spring Boot 회원가입 시 생성됩니다.
    Kaggle 시드 유저는 user_id = 'kaggle_{userId}' 형태로 구분됩니다.

    인증 관련 컬럼(password_hash, provider 등)은 DDL에 존재하지만,
    이 서비스에서는 읽기 전용으로만 매핑합니다.
    실제 인증 처리는 Spring Boot JWT 토큰으로 수행됩니다.
    """
    __tablename__ = "users"
    __table_args__ = {"extend_existing": True}

    # ── PK: 사용자 고유 식별자 ──
    user_id: str = Column(String(50), primary_key=True, comment="사용자 ID")
    # 닉네임
    nickname: str | None = Column(String(100), nullable=True, comment="닉네임")
    # 이메일
    email: str | None = Column(String(200), nullable=True, comment="이메일")
    # 프로필 이미지 URL
    profile_image: str | None = Column(String(500), nullable=True, comment="프로필 이미지 URL")
    # 연령대 (10대, 20대 등)
    age_group: str | None = Column(String(10), nullable=True, comment="연령대")
    # 성별 (M/F/O)
    gender: str | None = Column(String(10), nullable=True, comment="성별 (M/F/O)")
    # ── 인증/계정 관련 컬럼 (DDL 동기화, 이 서비스에서는 읽기 전용) ──
    # 비밀번호 해시 (BCrypt, 소셜 로그인 시 NULL)
    password_hash: str | None = Column(String(255), nullable=True, comment="비밀번호 (BCrypt, 소셜 로그인 시 NULL)")
    # 로그인 제공자 (LOCAL: 자체 가입, NAVER/KAKAO/GOOGLE: 소셜 로그인)
    provider: str = Column(String(20), nullable=False, default="LOCAL", comment="로그인 제공자 (LOCAL, NAVER, KAKAO, GOOGLE)")
    # 소셜 제공자 고유 ID (소셜 로그인 시 해당 플랫폼의 사용자 식별자)
    provider_id: str | None = Column(String(200), nullable=True, comment="소셜 제공자 고유 ID")
    # 사용자 역할 (USER: 일반 사용자, ADMIN: 관리자)
    user_role: str = Column(String(20), nullable=False, default="USER", comment="역할 (USER, ADMIN)")
    # 생년월일 (YYYYMMDD 형식, 선택 입력)
    user_birth: str | None = Column(String(20), nullable=True, comment="생년월일 (YYYYMMDD)")
    # 선택 약관 동의 여부 (마케팅 수신 등)
    option_term: bool = Column(Boolean, default=False, comment="선택 약관 동의 여부")
    # 필수 약관 동의 여부 (이용약관, 개인정보처리방침)
    required_term: bool = Column(Boolean, default=False, comment="필수 약관 동의 여부")


class UserPreference(Base):
    """
    사용자 선호도 엔티티 (읽기/쓰기)

    DDL: init.sql의 user_preferences 테이블과 동일한 구조입니다.
    온보딩 결과(장르 선택, 월드컵 결과, 무드 선택)를 이 테이블에 저장합니다.

    FK: user_id → users.user_id (ON DELETE CASCADE)
    """
    __tablename__ = "user_preferences"
    __table_args__ = {"extend_existing": True}

    # 실제 운영 스키마는 별도 id 없이 user_id 1:1 레코드로 관리됩니다.
    user_id: str = Column(String(50), primary_key=True, nullable=False, comment="사용자 ID")
    # 운영 DB 기준으로 실제 사용 중인 최소 컬럼만 매핑합니다.
    # recommend 온보딩은 현재 preferred_genres / preferred_moods만 읽고 씁니다.
    preferred_genres = Column(JSON, nullable=True, comment='선호 장르 ["액션","SF"]')
    preferred_moods = Column(JSON, nullable=True, comment='선호 무드 ["스릴","감동"]')


# =========================================
# 이 서비스 소유 테이블
# =========================================

class SearchHistory(Base):
    """
    검색 이력 엔티티

    검색어 입력과 결과 클릭 이벤트를 모두 저장합니다.
    최근 검색어 화면에서는 동일 키워드를 최신 시각 기준으로 한 번만 노출합니다.

    DDL: Backend JPA가 관리하는 search_history 테이블
    """
    __tablename__ = "search_history"
    __table_args__ = (
        # 사용자별 검색 시각 기준 내림차순 조회 최적화
        Index("idx_search_history_user_time", "user_id", "searched_at"),
    )

    # 2026-03-24 backend 스키마 변경: PK 컬럼명 id -> search_history_id
    # Python 속성명은 기존 호환을 위해 id 로 유지하고, 실제 DB 컬럼명만 맞춥니다.
    id: int = Column(
        "search_history_id",
        AutoIncrementBigInt,
        primary_key=True,
        autoincrement=True,
    )
    # 검색한 사용자 ID (VARCHAR(50))
    user_id: str = Column(String(50), nullable=False, index=True, comment="사용자 ID")
    # 검색 키워드 (공백 제거 후 저장)
    keyword: str = Column(String(200), nullable=False, comment="검색 키워드")
    # 검색 시각 (최신 검색 시 갱신)
    searched_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(),
        comment="검색 시각"
    )
    # 검색 결과 개수
    result_count: int | None = Column(Integer, nullable=True, comment="검색 결과 수")
    # 검색 결과에서 클릭한 영화 ID
    clicked_movie_id: str | None = Column(
        String(50), nullable=True, comment="클릭한 영화 ID"
    )
    # 검색 시 적용한 필터 정보
    filters = Column(JSON, nullable=True, comment="검색 필터 정보 JSON")


class TrendingKeyword(Base):
    """
    인기 검색어 엔티티

    전체 사용자의 검색 횟수를 집계하여 인기 검색어를 산출합니다.
    Redis Sorted Set으로 실시간 순위를 관리하고,
    이 테이블은 영속적인 백업/통계 분석용입니다.

    DDL: init.sql의 trending_keywords 테이블
    """
    __tablename__ = "trending_keywords"
    __table_args__ = (
        # 검색 횟수 기준 내림차순 정렬 최적화
        Index("idx_trending_count", "search_count"),
    )

    # 고유 식별자 (DDL: BIGINT AUTO_INCREMENT, SQLite 호환 variant)
    id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
    # 검색 키워드 (유니크)
    keyword: str = Column(String(200), nullable=False, unique=True, comment="검색 키워드")
    # 누적 검색 횟수
    search_count: int = Column(Integer, nullable=False, default=0, comment="누적 검색 횟수")
    # 마지막 검색 시각
    last_searched_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), comment="마지막 검색 시각"
    )


class WorldcupResult(Base):
    """
    이상형 월드컵 결과 엔티티

    사용자가 진행한 영화 이상형 월드컵의 최종 결과를 저장합니다.
    우승 영화, 준우승, 4강 영화 ID와 각 라운드별 선택 로그를 기록합니다.
    이 데이터를 기반으로 장르/키워드 선호도 레이더 차트를 생성합니다.

    DDL: backend JPA 기준 worldcup_results 테이블
    주의: movie_id FK 타입은 VARCHAR(50)입니다 (Integer 아님).
    """
    __tablename__ = "worldcup_results"
    __table_args__ = (
        Index("idx_worldcup_user", "user_id"),
    )

    # 고유 식별자 (DDL: BIGINT AUTO_INCREMENT, 컬럼명: worldcup_result_id)
    worldcup_result_id: int = Column(
        "worldcup_result_id",
        AutoIncrementBigInt,
        primary_key=True,
        autoincrement=True,
    )
    # 사용자 ID (VARCHAR(50))
    user_id: str = Column(String(50), nullable=False, comment="사용자 ID")
    # 라운드 수 (16 또는 32)
    round_size: int = Column(Integer, nullable=False, default=16, comment="라운드 크기")
    # 우승 영화 ID (VARCHAR(50))
    winner_movie_id: str = Column(String(50), nullable=False, comment="우승 영화 ID")
    # 준우승 영화 ID (VARCHAR(50), nullable)
    runner_up_movie_id: str | None = Column(String(50), nullable=True, comment="준우승 영화 ID")
    # 4강 영화 ID 목록 (JSON 배열)
    semi_final_movie_ids: str | None = Column(Text, nullable=True, comment="4강 영화 ID 목록 (JSON)")
    # 전체 라운드별 선택 로그 (JSON)
    selection_log: str | None = Column(Text, nullable=True, comment="라운드별 선택 로그 (JSON)")
    # 분석된 장르 선호도 (JSON: {"액션": 0.8, "로맨스": 0.5, ...})
    genre_preferences: str | None = Column(Text, nullable=True, comment="장르 선호도 (JSON)")
    # 온보딩 완료 여부
    onboarding_completed: bool = Column(Boolean, nullable=False, default=False, comment="온보딩 완료 여부")
    # worldcup_session 논리 참조
    session_id: int | None = Column(BigInteger, nullable=True, comment="월드컵 세션 ID")
    # 리워드 지급 여부
    reward_granted: bool = Column(Boolean, nullable=False, default=False, comment="리워드 지급 여부")
    # 전체 매치 수
    total_matches: int | None = Column(Integer, nullable=True, comment="전체 매치 수")
    # 생성 시각
    created_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), comment="생성 시각"
    )
    # 수정 시각
    updated_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment="수정 시각"
    )


class WorldcupSession(Base):
    """
    이상형 월드컵 세션 엔티티

    recommend 런타임이 월드컵 시작/진행/완료 상태를 영속화할 때 사용합니다.
    실제 진행 상세는 Redis를 계속 사용하지만, 세션의 시작 조건과 최종 상태는
    worldcup_session에 남겨 운영/분석 기준과 맞춥니다.
    """
    __tablename__ = "worldcup_session"
    __table_args__ = (
        Index("idx_session_user", "user_id", "status"),
        Index("idx_session_source", "source_type"),
        Index("idx_session_category", "category_id"),
        {"extend_existing": True},
    )

    session_id: int = Column(
        "session_id",
        AutoIncrementBigInt,
        primary_key=True,
        autoincrement=True,
    )
    user_id: str = Column(String(50), nullable=False, comment="사용자 ID")
    source_type: str = Column(String(20), nullable=False, comment="시작 방식 (CATEGORY / GENRE)")
    category_id: int | None = Column(BigInteger, nullable=True, comment="카테고리 ID")
    selected_genres_json: str | None = Column(Text, nullable=True, comment="선택 장르 JSON")
    candidate_pool_size: int = Column(Integer, nullable=False, comment="후보 풀 크기")
    round_size: int = Column(Integer, nullable=False, comment="총 라운드 크기")
    current_round: int = Column(Integer, nullable=False, comment="현재 라운드")
    current_match_order: int = Column(Integer, nullable=False, default=0, comment="현재 라운드 매치 순서")
    status: str = Column(String(20), nullable=False, default="IN_PROGRESS", comment="세션 상태")
    winner_movie_id: str | None = Column(String(50), nullable=True, comment="우승 영화 ID")
    started_at: datetime = Column(DateTime, nullable=False, default=func.now(), comment="시작 시각")
    completed_at: datetime | None = Column(DateTime, nullable=True, comment="완료 시각")
    reward_granted: bool = Column(Boolean, nullable=False, default=False, comment="리워드 지급 여부")
    created_at: datetime = Column(DateTime, nullable=False, default=func.now(), comment="생성 시각")
    updated_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment="수정 시각"
    )
    created_by: str | None = Column(String(50), nullable=True, comment="생성자")
    updated_by: str | None = Column(String(50), nullable=True, comment="수정자")


class WorldcupMatch(Base):
    """
    이상형 월드컵 매치 엔티티

    각 세션의 라운드별 대진과 선택 결과를 저장합니다.
    recommend는 라운드 단위 제출 구조이지만, 실제 매치 레코드는 DB에 남깁니다.
    """
    __tablename__ = "worldcup_match"
    __table_args__ = (
        Index("uk_session_round_order", "session_id", "round_number", "match_order", unique=True),
        {"extend_existing": True},
    )

    match_id: int = Column(
        "match_id",
        AutoIncrementBigInt,
        primary_key=True,
        autoincrement=True,
    )
    session_id: int = Column(BigInteger, nullable=False, comment="월드컵 세션 ID")
    round_number: int = Column(Integer, nullable=False, comment="라운드 번호")
    match_order: int = Column(Integer, nullable=False, comment="라운드 내 순서")
    movie_a_id: str = Column(String(50), nullable=False, comment="대결 영화 A ID")
    movie_b_id: str = Column(String(50), nullable=False, comment="대결 영화 B ID")
    winner_movie_id: str | None = Column(String(50), nullable=True, comment="선택된 승자 영화 ID")
    selected_at: datetime | None = Column(DateTime, nullable=True, comment="선택 완료 시각")
    created_at: datetime = Column(DateTime, nullable=False, default=func.now(), comment="생성 시각")
    updated_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment="수정 시각"
    )
    created_by: str | None = Column(String(50), nullable=True, comment="생성자")
    updated_by: str | None = Column(String(50), nullable=True, comment="수정자")


class WorldcupCategory(Base):
    """
    월드컵 카테고리 엔티티

    Backend JPA가 관리하는 worldcup_category 테이블을 읽기 전용에 가깝게 매핑합니다.
    온보딩 월드컵 시작 화면에서 노출 가능한 카테고리 목록 계산에 사용합니다.
    """
    __tablename__ = "worldcup_category"
    __table_args__ = {"extend_existing": True}

    category_id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
    category_code: str = Column(String(100), nullable=False, unique=True, comment="카테고리 코드")
    category_name: str = Column(String(100), nullable=False, comment="카테고리 이름")
    description: str | None = Column(Text, nullable=True, comment="카테고리 설명")
    admin_note: str | None = Column(Text, nullable=True, comment="관리자 메모")
    display_order: int = Column(Integer, nullable=False, default=0, comment="노출 순서")
    is_enabled: bool = Column(Boolean, nullable=False, default=True, comment="노출 여부")
    created_at: datetime = Column(DateTime, nullable=False, default=func.now(), comment="생성 시각")
    updated_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment="수정 시각"
    )
    created_by: str | None = Column(String(50), nullable=True, comment="생성자")
    updated_by: str | None = Column(String(50), nullable=True, comment="수정자")


class WorldcupCandidate(Base):
    """
    월드컵 후보 엔티티

    worldcup_candidate 테이블을 매핑합니다.
    카테고리 기반 월드컵의 후보 수 계산과 랜덤 후보 선택에 사용합니다.
    """
    __tablename__ = "worldcup_candidate"
    __table_args__ = (
        Index("uk_worldcup_candidate_movie_category", "movie_id", "category_id", unique=True),
        Index("idx_wcc_category", "category_id"),
        Index("idx_wcc_active", "is_active"),
        {"extend_existing": True},
    )

    id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
    movie_id: str = Column(String(50), nullable=False, comment="영화 ID")
    category_id: int = Column(BigInteger, nullable=False, comment="카테고리 ID")
    popularity: float | None = Column(Float, nullable=True, comment="인기도 스냅샷")
    is_active: bool = Column(Boolean, nullable=False, default=True, comment="활성 여부")
    added_by: str | None = Column(String(50), nullable=True, comment="등록자")
    created_at: datetime = Column(DateTime, nullable=False, default=func.now(), comment="생성 시각")
    updated_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment="수정 시각"
    )
    created_by: str | None = Column(String(50), nullable=True, comment="생성자")
    updated_by: str | None = Column(String(50), nullable=True, comment="수정자")


# =========================================
# 공유 테이블 — 등급/포인트/업적 (Backend JPA DDL 기준, 읽기 전용)
# 2026-03-31 동기화: UserGrade enum → grades 테이블, point_have → balance, achievement_type → FK
# =========================================

class Grade(Base):
    """
    사용자 등급 마스터 엔티티 (읽기 전용).

    Backend JPA: reward.entity.Grade — grades 테이블.

    2026-03-31 신규 추가:
      기존 UserGrade enum(BRONZE/SILVER/GOLD/PLATINUM) 고정값을 DB 테이블로 전환하여
      관리자 페이지에서 등급 기준(min_points)과 쿼터(daily_ai_limit 등)를 동적으로 변경할 수 있다.

    이 서비스에서는 등급별 쿼터 조회(읽기) 목적으로만 사용한다.
    DDL 변경 권한은 Backend JPA(ddl-auto=update)에 있으며, 이 모델은 읽기 전용이다.

    PK: grade_id BIGINT AUTO_INCREMENT (서로게이트 PK)
    UK: grade_code (BRONZE/SILVER/GOLD/PLATINUM)
    """
    __tablename__ = "grades"
    # extend_existing: 다른 모듈에서 동일 테이블이 이미 등록된 경우 재정의 방지
    __table_args__ = {"extend_existing": True}

    # ── PK: 등급 레코드 고유 ID (BIGINT AUTO_INCREMENT) ──
    grade_id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True,
                           comment="등급 ID (BIGINT AUTO_INCREMENT PK)")

    # 등급 코드 (UNIQUE, NOT NULL) — BRONZE / SILVER / GOLD / PLATINUM
    # UserGrade enum의 name()과 동일한 값이며 대문자로 관리한다.
    grade_code: str = Column(String(20), nullable=False, unique=True,
                             comment="등급 코드 (BRONZE/SILVER/GOLD/PLATINUM)")

    # 등급 한글 표시명 — 관리자 페이지 및 클라이언트 UI 노출용
    # 예: '브론즈', '실버', '골드', '플래티넘'
    grade_name: str | None = Column(String(50), nullable=True,
                                    comment="등급 한글 표시명 (예: 브론즈)")

    # 최소 누적 획득 포인트 — 이 값 이상이면 해당 등급 부여
    # 포인트를 소비해도 등급은 하락하지 않음 (누적 기준)
    min_points: int = Column(Integer, nullable=False, comment="등급 달성 최소 누적 포인트")

    # 일일 AI 추천 한도 (-1 이면 무제한, PLATINUM)
    daily_ai_limit: int | None = Column(Integer, nullable=True,
                                        comment="일일 AI 추천 한도 (-1=무제한)")

    # 월간 AI 추천 한도 (-1 이면 무제한, PLATINUM)
    monthly_ai_limit: int | None = Column(Integer, nullable=True,
                                          comment="월간 AI 추천 한도 (-1=무제한)")

    # 무료 일일 AI 추천 횟수 — 이 횟수까지 포인트 미차감
    free_daily_count: int | None = Column(Integer, nullable=True,
                                          comment="무료 일일 AI 추천 횟수")

    # 최대 입력 글자 수 — 등급이 높을수록 더 긴 메시지 허용
    max_input_length: int | None = Column(Integer, nullable=True,
                                          comment="최대 입력 글자 수")

    # 표시 정렬 순서 (오름차순, BRONZE=1 ... PLATINUM=4)
    sort_order: int | None = Column(Integer, nullable=True,
                                    comment="정렬 순서 (낮을수록 앞에 표시)")

    # 활성 여부 — false이면 쿼터 조회에서 제외
    is_active: bool = Column(Boolean, nullable=True, default=True,
                             comment="등급 활성 여부")


class UserPoint(Base):
    """
    사용자 포인트 잔액 엔티티 (읽기 전용).

    Backend JPA: reward.entity.UserPoint — user_points 테이블.

    2026-03-31 변경 사항 동기화:
      1. point_have (VARCHAR 시절 잔액 컬럼) → balance (INTEGER) 로 컬럼명 변경
      2. user_grade VARCHAR(ENUM 문자열) → grade_id BIGINT FK (→ grades.grade_id) 로 변경

    이 서비스에서는 사용자 잔액 및 등급 조회(읽기) 목적으로만 사용한다.
    포인트 차감/적립은 Backend REST API를 통해서만 수행한다.

    PK: user_point_id BIGINT AUTO_INCREMENT (서로게이트 PK)
    UK: user_id (사용자 1명당 포인트 레코드 1개)
    FK: grade_id → grades.grade_id (LAZY, null=BRONZE fallback)
    """
    __tablename__ = "user_points"
    __table_args__ = {"extend_existing": True}

    # ── PK: 포인트 레코드 고유 ID (BIGINT AUTO_INCREMENT) ──
    # 기존 필드명: point_id → user_point_id (Backend JPA 2026-03-24 변경)
    user_point_id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True,
                                comment="포인트 레코드 ID (BIGINT AUTO_INCREMENT PK)")

    # 사용자 ID (VARCHAR(50), NOT NULL, UNIQUE)
    # users.user_id를 참조하며, 사용자 1명당 반드시 1개만 존재해야 한다.
    user_id: str = Column(String(50), nullable=False, unique=True,
                          comment="사용자 ID (UK)")

    # ── 잔액 컬럼 (2026-03-31 변경: point_have → balance) ──
    # Backend JPA UserPoint.balance 필드와 동일한 컬럼명.
    # 이전 컬럼명 'point_have'는 더 이상 사용하지 않는다.
    balance: int = Column(Integer, nullable=True, default=0,
                          comment="현재 보유 포인트 (구 point_have)")

    # 누적 획득 포인트 (가입 이후 전체 합산, 등급 판정 기준)
    total_earned: int = Column(Integer, nullable=True, default=0,
                               comment="누적 획득 포인트 (등급 판정 기준)")

    # 오늘 획득 포인트 (일일 한도 관리용)
    daily_earned: int = Column(Integer, nullable=True, default=0,
                               comment="오늘 획득 포인트 (일일 한도 관리용)")

    # 일일 리셋 기준일 (날짜가 바뀌면 daily_earned를 0으로 초기화)
    daily_reset: datetime | None = Column(DateTime, nullable=True,
                                          comment="일일 리셋 기준일")

    # ── 등급 FK (2026-03-31 변경: user_grade VARCHAR → grade_id BIGINT FK) ──
    # Backend JPA @ManyToOne(fetch=LAZY) @JoinColumn(name="grade_id")와 동일.
    # 이 서비스에서는 FK 정수값만 저장하며, Grade 엔티티 조인이 필요할 때 별도 쿼리를 사용한다.
    # null이면 서비스 레이어에서 BRONZE 등급으로 fallback 처리한다.
    grade_id: int | None = Column(AutoIncrementBigInt, nullable=True,
                                  comment="등급 ID FK (→ grades.grade_id, 구 user_grade)")


class AchievementType(Base):
    """
    업적 유형 마스터 엔티티 (읽기 전용).

    Backend JPA: roadmap.entity.AchievementType — achievement_types 테이블.

    2026-03-31 신규 추가:
      기존 user_achievements.achievement_type VARCHAR(50) 단일 컬럼을
      achievement_types 마스터 테이블 + FK 구조로 분리.
      업적 메타정보(표시명·보상·아이콘 등)를 DB에서 동적으로 관리할 수 있다.

    이 서비스에서는 업적 유형 메타데이터 조회(읽기) 목적으로만 사용한다.

    PK: achievement_type_id BIGINT AUTO_INCREMENT (서로게이트 PK)
    UK: achievement_code (예: "course_complete", "quiz_perfect")

    기본 업적 유형 (앱 시작 시 AchievementInitializer에서 INSERT):
      - course_complete  : 도장깨기 코스 완주 (보상 100P)
      - quiz_perfect     : 퀴즈 만점 달성 (보상 50P)
      - review_count_10  : 리뷰 10개 작성 (보상 200P)
      - genre_explorer   : 5개 장르 탐험 (보상 150P)
    """
    __tablename__ = "achievement_types"
    __table_args__ = {"extend_existing": True}

    # ── PK: 업적 유형 고유 ID (BIGINT AUTO_INCREMENT) ──
    # user_achievements.achievement_type_id FK가 이 값을 참조한다.
    achievement_type_id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True,
                                      comment="업적 유형 ID (BIGINT AUTO_INCREMENT PK)")

    # 업적 코드 — 시스템 내부 식별자 (UNIQUE, NOT NULL)
    # 영문 소문자+언더스코어 형식. 서비스 로직에서 업적 달성 판정 시 이 값으로 조회한다.
    # 예: "course_complete", "quiz_perfect", "review_count_10", "genre_explorer"
    achievement_code: str = Column(String(50), nullable=False, unique=True,
                                   comment="업적 코드 (예: course_complete)")

    # 업적 표시명 — 한국어 사용자 화면 노출 이름 (NOT NULL)
    # 예: "코스 완주", "퀴즈 만점", "리뷰 10개 달성", "5개 장르 탐험"
    achievement_name: str = Column(String(100), nullable=False,
                                   comment="업적 표시명 (한국어)")

    # 업적 설명 — 달성 조건 및 내용 (선택)
    description: str | None = Column(String(500), nullable=True,
                                     comment="업적 설명 및 달성 조건")

    # 달성 조건 횟수 (선택 — null이면 1회 달성 완료형)
    # 예: review_count_10 → 10, genre_explorer → 5
    required_count: int | None = Column(Integer, nullable=True,
                                        comment="달성 조건 횟수 (null=1회)")

    # 업적 달성 시 지급되는 보상 포인트 (선택 — null이면 포인트 보상 없음)
    reward_points: int | None = Column(Integer, nullable=True,
                                       comment="달성 보상 포인트 (null=없음)")

    # 업적 아이콘 URL (선택) — 프론트엔드에서 배지 이미지 렌더링에 사용
    icon_url: str | None = Column(String(500), nullable=True,
                                  comment="업적 아이콘 URL")

    # 활성 여부 — false이면 새 달성 기록이 생성되지 않음 (기존 기록 보존)
    is_active: bool = Column(Boolean, nullable=True, default=True,
                             comment="업적 활성 여부 (false=신규 달성 불가)")
