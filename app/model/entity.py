"""
SQLAlchemy ORM 엔티티 모델 정의

DDL 기준: monglepick-agent/init.sql (18개 테이블)
AI Agent가 DDL을 관리하며, 이 서비스는 해당 스키마에 맞춰 매핑합니다.

공유 테이블 (AI Agent DDL 기준, 읽기 전용):
- movies: 영화 경량 참조 (PK: movie_id VARCHAR(50))
- users: 사용자 기본 정보 (PK: user_id VARCHAR(50))
- user_preferences: 사용자 취향 프로필 (읽기/쓰기)

이 서비스가 소유하는 테이블:
- search_history: 사용자별 최근 검색 이력
- trending_keywords: 인기 검색어 집계
- worldcup_results: 이상형 월드컵 결과 저장

주의: 모든 PK/FK 타입은 DDL 기준 VARCHAR(50)입니다.
      Integer PK를 사용하지 않습니다.
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
# 공유 테이블 (AI Agent DDL 기준, 읽기 전용 매핑)
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
    cast = Column(JSON, nullable=True, comment='주연 배우 목록 ["배우1","배우2"]')
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
        if not self.cast:
            return []
        if isinstance(self.cast, list):
            return self.cast
        try:
            return json.loads(self.cast)
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

    # 선호도 고유 식별자 (DDL: BIGINT AUTO_INCREMENT, SQLite 호환 variant)
    id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
    # 사용자 FK (VARCHAR(50), UNIQUE)
    user_id: str = Column(String(50), nullable=False, unique=True, comment="사용자 ID")
    # ── 선호 조건 (모두 JSON 배열) ──
    # 선호 장르 (예: ["액션", "SF"])
    preferred_genres = Column(JSON, nullable=True, comment='선호 장르 ["액션","SF"]')
    # 선호 무드 (예: ["스릴", "감동"])
    preferred_moods = Column(JSON, nullable=True, comment='선호 무드 ["스릴","감동"]')
    # 선호 감독 (예: ["봉준호"])
    preferred_directors = Column(JSON, nullable=True, comment='선호 감독 ["봉준호"]')
    # 선호 배우 (예: ["송강호"])
    preferred_actors = Column(JSON, nullable=True, comment='선호 배우 ["송강호"]')
    # 선호 시대 (예: ["2020s"])
    preferred_eras = Column(JSON, nullable=True, comment='선호 시대 ["2020s"]')
    # 제외 장르 (예: ["호러"])
    excluded_genres = Column(JSON, nullable=True, comment='제외 장르 ["호러"]')
    # 선호 OTT 플랫폼 (예: ["넷플릭스"])
    preferred_platforms = Column(JSON, nullable=True, comment='선호 OTT ["넷플릭스"]')
    # 선호 관람등급
    preferred_certification: str | None = Column(String(50), nullable=True, comment="선호 관람등급")
    # 추가 선호 조건 (키-값 자유 형식)
    extra_preferences = Column(JSON, nullable=True, comment="추가 선호 조건 (키-값 자유 형식)")


# =========================================
# 이 서비스 소유 테이블
# =========================================

class SearchHistory(Base):
    """
    검색 이력 엔티티

    사용자별 최근 검색어를 저장합니다.
    최대 20건까지 보관하며, 오래된 검색어는 자동 삭제됩니다.
    동일 키워드 재검색 시 타임스탬프만 갱신합니다.

    DDL: init.sql의 search_history 테이블
    """
    __tablename__ = "search_history"
    __table_args__ = (
        # 사용자별 검색 시각 기준 내림차순 조회 최적화
        Index("idx_search_history_user_time", "user_id", "searched_at"),
        # 동일 사용자의 동일 키워드 중복 방지
        Index("uk_search_history_user_keyword", "user_id", "keyword", unique=True),
    )

    # 검색 이력 고유 식별자 (DDL: BIGINT AUTO_INCREMENT, SQLite 호환 variant)
    id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
    # 검색한 사용자 ID (VARCHAR(50))
    user_id: str = Column(String(50), nullable=False, index=True, comment="사용자 ID")
    # 검색 키워드 (공백 제거 후 저장)
    keyword: str = Column(String(200), nullable=False, comment="검색 키워드")
    # 검색 시각 (최신 검색 시 갱신)
    searched_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now(),
        comment="검색 시각"
    )


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

    DDL: init.sql의 worldcup_results 테이블
    주의: movie_id FK 타입은 VARCHAR(50)입니다 (Integer 아님).
    """
    __tablename__ = "worldcup_results"
    __table_args__ = (
        Index("idx_worldcup_user", "user_id"),
    )

    # 고유 식별자 (DDL: BIGINT AUTO_INCREMENT, SQLite 호환 variant)
    id: int = Column(AutoIncrementBigInt, primary_key=True, autoincrement=True)
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
    # 생성 시각
    created_at: datetime = Column(
        DateTime, nullable=False, default=func.now(), comment="생성 시각"
    )
