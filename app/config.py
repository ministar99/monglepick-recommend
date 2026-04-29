"""
몽글픽 추천 서비스 설정 모듈

pydantic-settings를 사용하여 환경변수에서 설정값을 로드합니다.
.env 파일 또는 시스템 환경변수에서 자동으로 읽어옵니다.

Spring Boot 백엔드(monglepick-backend)와 공유하는 설정:
- MySQL 접속 정보 (동일 DB 사용)
- JWT 시크릿 키 (동일 토큰 검증)
"""

from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    애플리케이션 설정 클래스

    환경변수 또는 .env 파일에서 값을 읽어옵니다.
    민감한 값(DB 계정, JWT 시크릿)은 반드시 .env 또는 시스템 환경변수로 주입합니다.
    """

    # -----------------------------------------
    # 애플리케이션 기본 설정
    # -----------------------------------------
    APP_NAME: str = Field(...)
    APP_VERSION: str = Field(...)
    DEBUG: str = Field(...)
    API_V1_PREFIX: str = Field(...)

    # -----------------------------------------
    # SQL 로깅 (개발/디버깅용)
    # -----------------------------------------
    # True 로 설정하면 v1(SQLAlchemy)과 v2(aiomysql Raw SQL) 양쪽 모두
    # 실행된 쿼리를 logger 로 출력한다.
    # - v1: SQLAlchemy echo=True 와 동일 (sqlalchemy.engine 로거 INFO 레벨)
    # - v2: aiomysql DictCursor/Cursor 를 LoggingDictCursor/LoggingCursor 로 교체
    # 운영에서는 반드시 False 유지 (성능 및 민감정보 로그 누출 방지)
    SQL_ECHO: bool = Field(default=False)

    # -----------------------------------------
    # MySQL 설정 (Spring Boot 백엔드와 공유)
    # -----------------------------------------
    DB_HOST: str = Field(...)
    DB_PORT: str = Field(...)
    DB_NAME: str = Field(...)
    DB_USERNAME: str = Field(
        ...,
        validation_alias=AliasChoices("DB_USERNAME", "DB_USER"),
    )
    DB_PASSWORD: str = Field(...)

    @property
    def database_url(self) -> str:
        """SQLAlchemy 비동기 MySQL 접속 URL을 생성합니다."""
        return (
            f"mysql+aiomysql://{self.DB_USERNAME}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            f"?charset=utf8mb4"
        )

    # -----------------------------------------
    # Redis 설정
    # -----------------------------------------
    REDIS_HOST: str = Field(...)
    REDIS_PORT: int = Field(...)
    REDIS_DB: int = Field(...)  # 0번은 monglepick-agent가 사용, 1번 사용

    @property
    def redis_url(self) -> str:
        """Redis 접속 URL을 생성합니다."""
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # -----------------------------------------
    # JWT 설정 (Spring Boot 백엔드와 동일 시크릿)
    # -----------------------------------------
    JWT_SECRET: str = Field(...)
    JWT_ALGORITHM: str = Field(...)

    # -----------------------------------------
    # 서버 설정
    # -----------------------------------------
    SERVER_HOST: str = Field(...)
    SERVER_PORT: int = Field(...)

    # -----------------------------------------
    # CORS 설정
    # -----------------------------------------
    CORS_ORIGINS: str = Field(...)

    @property
    def cors_origin_list(self) -> list[str]:
        """CORS 허용 오리진을 리스트로 변환합니다."""
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]

    # -----------------------------------------
    # TMDB 이미지 URL
    # -----------------------------------------
    TMDB_IMAGE_BASE_URL: str = Field(...)

    # -----------------------------------------
    # 검색 관련 설정
    # -----------------------------------------
    # 자동완성 Redis 캐시 TTL (초)
    AUTOCOMPLETE_CACHE_TTL: int = 300  # 5분
    # 검색용 Elasticsearch 활성화 여부
    SEARCH_ES_ENABLED: bool = False
    # 검색용 Elasticsearch 접속 URL
    ELASTICSEARCH_URL: str | None = None
    # 검색용 Elasticsearch 인덱스명
    ELASTICSEARCH_INDEX: str = Field(...)
    # 연관 영화 조회용 Qdrant 로컬/SSH 터널 URL
    QDRANT_URL: str = Field(...)
    # 연관 영화 조회용 Qdrant 컬렉션명
    QDRANT_COLLECTION: str = "movies"
    # 연관 영화 조회용 Neo4j HTTP 엔드포인트 (읽기 전용 Cypher 실행)
    NEO4J_HTTP_URL: str = Field(...)
    # Neo4j 기본 계정 — 로컬/SSH 터널 환경에서 env로 덮어쓸 수 있다.
    NEO4J_USER: str = Field(...)
    # Neo4j 기본 비밀번호 — 로컬 가이드 값을 기본으로 두되 env가 우선한다.
    NEO4J_PASSWORD: str = Field(...)
    # 외부 DB(Qdrant/Neo4j) 조회 타임아웃 (초)
    RELATED_MOVIE_HTTP_TIMEOUT_SEC: float = 4.0
    # 영화 상세 연관 영화 기본/최대 노출 개수
    RELATED_MOVIES_LIMIT: int = 25
    # 영화 상세 연관 영화 최종 응답 Redis 캐시 TTL (초)
    RELATED_MOVIES_CACHE_TTL: int = 300
    # 인기 검색어 집계 기간 (시간)
    TRENDING_WINDOW_HOURS: int = 24
    # 인기 검색어 표시 개수
    TRENDING_TOP_K: int = 10
    # 최근 검색어 한 페이지 최대 반환 건수 (offset 페이지네이션 상한).
    # 30건은 너무 많으니까 10건으로 줄임. 바꾸지마시오!!!!
    RECENT_SEARCH_MAX: int = 10
    # 장르 탐색 검색 최소 평점 참여 인원 수
    GENRE_DISCOVERY_MIN_VOTE_COUNT: int = 100

    # -----------------------------------------
    # 온보딩 설정
    # -----------------------------------------
    # 월드컵 라운드 옵션 (16강 또는 32강)
    WORLDCUP_ROUNDS: list[int] = [16, 32]
    # 최소 선택 장르 수
    MIN_GENRE_SELECTION: int = 3

    # -----------------------------------------
    # 영화 좋아요 (write-behind 캐시) 설정
    # 2026-04-07 신규: Backend monglepick-backend에서 recommend로 이관
    # Redis dirty 큐를 주기적으로 드레인하여 MySQL에 배치 반영한다.
    # -----------------------------------------
    # dirty 큐 flush 주기 (초). 짧을수록 데이터 손실 위험이 줄지만 DB 부하 증가.
    # 기본값 60초 = 최악의 경우 1분치 토글이 Redis 장애 시 손실 가능.
    LIKE_FLUSH_INTERVAL_SECONDS: int = 60
    # flush 스케줄러 활성화 여부 (테스트 환경에서는 False로 두어 무한 루프 방지)
    LIKE_FLUSH_ENABLED: bool = True
    # 한 번의 flush 배치에서 최대 처리 가능한 dirty 엔트리 수 (과도한 한 배치 방지)
    LIKE_FLUSH_BATCH_MAX: int = 1000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """
    Settings 싱글턴 인스턴스를 반환합니다.

    lru_cache 데코레이터로 한 번만 생성되며,
    이후 호출에서는 캐시된 인스턴스를 반환합니다.
    """
    return Settings()
