"""
Pydantic 요청/응답 스키마 정의

FastAPI 엔드포인트에서 사용하는 모든 요청 바디(Request)와
응답 바디(Response) 모델을 정의합니다.

네이밍 규칙:
- 요청: *Request (예: GenreSelectionRequest)
- 응답: *Response (예: MovieSearchResponse)
- 내부 DTO: 접미어 없음 (예: MovieBrief, GenreWithMovies)
test
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# =========================================
# 공통 모델
# =========================================

class MovieBrief(BaseModel):
    """
    영화 간략 정보 (검색 결과, 월드컵 후보 등에서 사용)

    DDL 기준: movie_id VARCHAR(50) PK, release_year INT
    포스터 URL은 TMDB 이미지 기본 URL + poster_path로 조합합니다.
    """
    movie_id: str = Field(description="영화 ID (VARCHAR(50), TMDB/KOBIS/KMDb)")
    title: str = Field(description="한국어 제목")
    title_en: str | None = Field(default=None, description="영어 원제")
    genres: list[str] = Field(default_factory=list, description="장르 목록")
    release_year: int | None = Field(default=None, description="개봉 연도")
    rating: float | None = Field(default=None, description="평균 평점 (0.0~10.0)")
    # 장르 탐색(vote_count>=100) 필터링 및 클라이언트에서 평점 참여 수 표시에 사용.
    # H4NN4N PR #28 테스트(test_search_movies_by_selected_genres_without_keyword)가
    # 응답의 vote_count 필드를 참조하므로 MovieBrief에도 노출한다.
    vote_count: int | None = Field(default=None, description="평점 참여 인원 수")
    poster_url: str | None = Field(default=None, description="포스터 이미지 전체 URL")
    trailer_url: str | None = Field(default=None, description="예고편 URL")
    overview: str | None = Field(default=None, description="줄거리 요약")

    model_config = ConfigDict(from_attributes=True)


class MovieDetailResponse(BaseModel):
    """
    영화 상세 정보 응답

    상세 페이지에서 사용하는 영화 단건 조회 응답입니다.
    movies 테이블의 주요 메타 정보를 포함하고,
    포스터/배경 이미지 URL은 TMDB 베이스 URL과 조합된 값을 반환합니다.
    """
    movie_id: str = Field(description="영화 ID (VARCHAR(50), TMDB/KOBIS/KMDb)")
    title: str = Field(description="한국어 제목")
    original_title: str | None = Field(default=None, description="영어 원제")
    genres: list[str] = Field(default_factory=list, description="장르 목록")
    release_year: int | None = Field(default=None, description="개봉 연도")
    release_date: str | None = Field(default=None, description="개봉일 (YYYY-MM-DD)")
    runtime: int | None = Field(default=None, description="상영 시간 (분)")
    rating: float | None = Field(default=None, description="평균 평점 (0.0~10.0)")
    vote_count: int | None = Field(default=None, description="투표 수")
    popularity_score: float | None = Field(default=None, description="TMDB 인기도 점수")
    poster_url: str | None = Field(default=None, description="포스터 이미지 전체 URL")
    backdrop_url: str | None = Field(default=None, description="배경 이미지 전체 URL")
    director: str | None = Field(default=None, description="감독")
    cast: list[str] = Field(default_factory=list, description="출연진 목록")
    certification: str | None = Field(default=None, description="관람 등급")
    trailer_url: str | None = Field(default=None, description="예고편 URL")
    overview: str | None = Field(default=None, description="줄거리")
    tagline: str | None = Field(default=None, description="태그라인")
    imdb_id: str | None = Field(default=None, description="IMDb ID")
    original_language: str | None = Field(default=None, description="원본 언어 코드")
    collection_name: str | None = Field(default=None, description="프랜차이즈/컬렉션 이름")
    kobis_open_dt: str | None = Field(default=None, description="KOBIS 개봉일 (YYYYMMDD)")
    awards: str | None = Field(default=None, description="수상 내역")
    filming_location: str | None = Field(default=None, description="촬영 장소")
    source: str | None = Field(default=None, description="데이터 출처")


class PaginationMeta(BaseModel):
    """페이지네이션 메타 정보"""
    page: int = Field(description="현재 페이지 번호 (1부터 시작)")
    size: int = Field(description="페이지당 항목 수")
    total: int = Field(description="전체 항목 수")
    total_pages: int = Field(description="전체 페이지 수")


# =========================================
# 검색 관련 스키마 (REQ_031~034)
# =========================================

class MovieSearchResponse(BaseModel):
    """
    영화 검색 응답

    검색 결과 목록과 페이지네이션 메타 정보를 포함합니다.
    필터(장르, 연도, 평점, 국가)와 정렬 옵션이 적용된 결과입니다.
    """
    movies: list[MovieBrief] = Field(description="검색 결과 영화 목록")
    pagination: PaginationMeta = Field(description="페이지네이션 정보")
    did_you_mean: str | None = Field(default=None, description="오타 교정 추천어")
    related_queries: list[str] = Field(default_factory=list, description="관련 추천 검색어 목록")
    search_source: str | None = Field(
        default=None,
        description="실제 검색에 사용된 소스 (elasticsearch/mysql/null)",
    )


class AutocompleteResponse(BaseModel):
    """
    자동완성 응답

    사용자가 입력 중인 키워드에 대한 자동완성 후보를 반환합니다.
    최대 10건, Redis 캐시 사용 (TTL 5분).
    """
    suggestions: list[str] = Field(description="자동완성 키워드 후보 목록 (최대 10건)")
    did_you_mean: str | None = Field(default=None, description="오타 교정 추천어")


class TrendingKeywordItem(BaseModel):
    """인기 검색어 개별 항목"""
    rank: int = Field(description="순위 (1부터 시작)")
    keyword: str = Field(description="검색 키워드")
    search_count: int = Field(description="검색 횟수")


class TrendingResponse(BaseModel):
    """인기 검색어 TOP 10 응답"""
    keywords: list[TrendingKeywordItem] = Field(description="인기 검색어 목록")


class AdminPopularKeywordItem(BaseModel):
    """관리자 검색 분석 탭용 인기 검색어 항목"""
    keyword: str = Field(description="검색 키워드")
    search_count: int = Field(description="trending_keywords 기준 누적 검색 수")
    conversion_rate: float = Field(description="검색 후 결과 클릭 전환율 (0.0~1.0)")


class AdminPopularKeywordsResponse(BaseModel):
    """관리자 검색 분석 탭용 인기 검색어 목록 응답"""
    keywords: list[AdminPopularKeywordItem] = Field(description="인기 검색어 목록")


class SearchGenreOption(BaseModel):
    """검색 페이지 장르 선택 옵션"""
    label: str = Field(description="사용자에게 노출할 장르 라벨")
    aliases: list[str] = Field(description="movies.genres JSON 매칭에 사용할 실제 장르명 목록")
    contents_count: int = Field(description="병합/정제 이후 장르별 컨텐츠 수")


class SearchGenreOptionsResponse(BaseModel):
    """검색 페이지 장르 옵션 목록 응답"""
    genres: list[SearchGenreOption] = Field(description="검색용 장르 옵션 목록")


class RecentSearchItem(BaseModel):
    """최근 검색어 개별 항목"""
    keyword: str = Field(description="검색 키워드")
    searched_at: datetime = Field(description="검색 시각")
    filters: dict[str, Any] | None = Field(default=None, description="검색 시 적용한 필터 정보")


class RecentSearchPagination(BaseModel):
    """최근 검색어 페이지네이션 메타 정보"""
    offset: int = Field(default=0, description="현재 조회 시작 위치")
    limit: int = Field(default=10, description="현재 페이지 크기")
    has_more: bool = Field(default=False, description="다음 페이지 존재 여부")
    next_offset: int | None = Field(default=None, description="다음 조회 시작 위치")


class RecentSearchResponse(BaseModel):
    """사용자의 최근 검색어 목록 응답 (중복 제거 후 페이지당 최대 10건)"""
    searches: list[RecentSearchItem] = Field(description="최근 검색어 목록")
    pagination: RecentSearchPagination = Field(
        default_factory=RecentSearchPagination,
        description="최근 검색어 페이지네이션 정보",
    )


class SearchClickLogRequest(BaseModel):
    """검색 결과 클릭 로그 저장 요청"""
    keyword: str = Field(min_length=1, max_length=200, description="검색 키워드")
    clicked_movie_id: str = Field(min_length=1, max_length=50, description="클릭한 영화 ID")
    result_count: int = Field(ge=0, description="검색 결과 수")
    filters: dict[str, Any] | None = Field(
        default=None,
        description="검색 시 적용한 필터 정보",
    )


class SearchClickLogResponse(BaseModel):
    """검색 결과 클릭 로그 저장 응답"""
    saved: bool = Field(description="저장 여부")
    message: str = Field(description="처리 결과 메시지")


# =========================================
# 온보딩 관련 스키마 (REQ_016~019)
# =========================================

class GenreWithMovies(BaseModel):
    """
    장르별 대표 영화 포스터 정보

    온보딩 1단계에서 장르 선택 UI에 표시할 데이터입니다.
    각 장르마다 대표 영화 3~5편의 포스터를 포함합니다.
    """
    genre: str = Field(description="장르명 (예: 액션, 로맨스)")
    representative_movies: list[MovieBrief] = Field(
        description="해당 장르의 대표 영화 목록 (포스터 표시용)"
    )


class GenreListResponse(BaseModel):
    """장르 목록 + 대표 영화 포스터 응답"""
    genres: list[GenreWithMovies] = Field(description="장르별 대표 영화 목록")


class GenreSelectionRequest(BaseModel):
    """
    장르 선택 요청

    사용자가 온보딩 1단계에서 선택한 호감 장르 목록입니다.
    최소 3개 이상 선택해야 합니다.
    """
    selected_genres: list[str] = Field(
        min_length=3,
        description="선택한 장르 목록 (최소 3개)",
        examples=[["액션", "SF", "스릴러"]],
    )


class GenreSelectionResponse(BaseModel):
    """장르 선택 저장 완료 응답"""
    message: str = Field(description="처리 결과 메시지")
    selected_genres: list[str] = Field(description="저장된 장르 목록")


class WorldcupSourceType(str, Enum):
    """월드컵 후보 산정 방식"""
    CATEGORY = "CATEGORY"
    GENRE = "GENRE"


class WorldcupCategoryOptionResponse(BaseModel):
    """사용자에게 노출할 월드컵 카테고리 옵션"""
    categoryId: int = Field(description="월드컵 카테고리 ID")
    categoryCode: str = Field(description="카테고리 코드")
    categoryName: str = Field(description="카테고리 이름")
    description: str | None = Field(default=None, description="카테고리 설명")
    displayOrder: int = Field(default=0, description="노출 순서")
    candidatePoolSize: int = Field(description="활성 후보 영화 수")
    availableRoundSizes: list[int] = Field(description="선택 가능한 라운드 크기")
    previewMovieId: str | None = Field(default=None, description="카드 미리보기 영화 ID")
    previewPosterUrl: str | None = Field(default=None, description="카드 미리보기 포스터 URL")
    isReady: bool = Field(description="16강 이상 진행 가능한지 여부")


class WorldcupGenreOptionResponse(BaseModel):
    """커스텀 월드컵 빌더용 장르 옵션"""
    genreCode: str = Field(description="장르 코드")
    genreName: str = Field(description="장르명")
    contentsCount: int = Field(description="해당 장르 컨텐츠 수")


class WorldcupStartOptionsRequest(BaseModel):
    """월드컵 시작 전 후보 풀과 가능 라운드 계산 요청"""
    sourceType: WorldcupSourceType = Field(description="시작 방식 (CATEGORY / GENRE)")
    categoryId: int | None = Field(default=None, description="카테고리 ID (CATEGORY일 때 사용)")
    selectedGenres: list[str] | None = Field(default=None, description="선택 장르 목록 (GENRE일 때 사용)")


class WorldcupStartOptionsResponse(BaseModel):
    """월드컵 시작 전 후보 풀과 가능 라운드 계산 응답"""
    sourceType: WorldcupSourceType = Field(description="시작 방식")
    categoryId: int | None = Field(default=None, description="카테고리 ID")
    selectedGenres: list[str] = Field(default_factory=list, description="선택 장르 목록")
    candidatePoolSize: int = Field(description="후보 풀 크기")
    availableRoundSizes: list[int] = Field(description="선택 가능한 라운드 크기")


class WorldcupStartRequest(BaseModel):
    """월드컵 시작 요청"""
    sourceType: WorldcupSourceType = Field(description="시작 방식 (CATEGORY / GENRE)")
    categoryId: int | None = Field(default=None, description="카테고리 ID (CATEGORY일 때 사용)")
    selectedGenres: list[str] | None = Field(default=None, description="선택 장르 목록 (GENRE일 때 사용)")
    roundSize: int = Field(description="라운드 크기 (8/16/32/64)")


class WorldcupCandidate(BaseModel):
    """월드컵 대진표의 개별 영화 후보"""
    movie: MovieBrief = Field(description="영화 정보")
    seed: int = Field(description="시드 번호 (대진표 배치용)")


class WorldcupMatch(BaseModel):
    """월드컵 개별 매치 (2개 영화 대결)"""
    match_id: int = Field(description="매치 고유 번호")
    movie_a: MovieBrief = Field(description="A 영화")
    movie_b: MovieBrief = Field(description="B 영화")


class WorldcupBracketResponse(BaseModel):
    """
    월드컵 대진표 응답

    카테고리 또는 장르 기반으로 8강/16강/32강/64강 후보를 생성합니다.
    각 매치는 2개 영화의 대결로 구성됩니다.
    """
    round_size: int = Field(description="라운드 크기 (8/16/32/64)")
    matches: list[WorldcupMatch] = Field(description="매치 목록")
    total_rounds: int = Field(description="총 진행 라운드 수 (예: 16강→8강→4강→결승 = 4)")


class WorldcupSelectionRequest(BaseModel):
    """
    월드컵 라운드별 선택 결과 제출 요청

    각 매치에서 사용자가 선택한 영화 ID를 전송합니다.
    클라이언트에서 한 라운드가 끝날 때마다 제출하거나,
    전체 월드컵 완료 후 일괄 제출할 수 있습니다.
    """
    round_size: int = Field(description="현재 라운드 크기 (예: 16, 8, 4, 2)")
    selections: list[str] = Field(
        description="각 매치에서 선택한 영화 ID 목록 (순서대로, VARCHAR(50))"
    )
    is_final: bool = Field(
        default=False,
        description="결승전 여부 (True이면 월드컵 종료)"
    )


class WorldcupSelectionResponse(BaseModel):
    """월드컵 라운드 선택 결과 응답"""
    message: str = Field(description="처리 결과")
    next_round: int | None = Field(
        default=None,
        description="다음 라운드 크기 (None이면 월드컵 종료)"
    )
    next_matches: list[WorldcupMatch] | None = Field(
        default=None,
        description="다음 라운드 매치 목록 (종료 시 None)"
    )


class GenrePreference(BaseModel):
    """장르별 선호도 점수 (레이더 차트용)"""
    genre: str = Field(description="장르명")
    score: float = Field(description="선호도 점수 (0.0~1.0)")


class WorldcupResultResponse(BaseModel):
    """
    월드컵 결과 분석 응답

    우승/준우승 영화와 장르별 선호도 레이더 차트 데이터를 포함합니다.
    이 데이터는 user_preferences 테이블에도 반영됩니다.
    """
    winner: MovieBrief = Field(description="우승 영화")
    runner_up: MovieBrief | None = Field(default=None, description="준우승 영화")
    genre_preferences: list[GenrePreference] = Field(
        description="장르별 선호도 (레이더 차트 데이터)"
    )
    top_genres: list[str] = Field(description="상위 선호 장르 (3개)")


class MoodTag(BaseModel):
    """무드 태그 항목"""
    id: int = Field(description="무드 태그 ID")
    name: str = Field(description="무드 태그명 (예: 긴장감있는, 감동적인)")
    emoji: str = Field(description="대표 이모지")


class MoodListResponse(BaseModel):
    """무드 태그 목록 응답"""
    moods: list[MoodTag] = Field(description="사용 가능한 무드 태그 목록")


class MoodSelectionRequest(BaseModel):
    """무드 기반 초기 설정 저장 요청"""
    selected_moods: list[str] = Field(
        min_length=1,
        description="선택한 무드 태그 목록",
        examples=[["긴장감있는", "감동적인", "유쾌한"]],
    )


class MoodSelectionResponse(BaseModel):
    """무드 선택 저장 완료 응답"""
    message: str = Field(description="처리 결과")
    selected_moods: list[str] = Field(description="저장된 무드 목록")


# =========================================
# 영화 좋아요 관련 스키마 (movie Like 도메인)
# =========================================
# Backend(Spring Boot) `LikeResponse` record와 JSON 키(camelCase)까지 1:1 일치하도록
# populate_by_name + alias를 사용한다. 클라이언트는 `liked` / `likeCount` 필드를
# 그대로 읽을 수 있으므로, 이관 전후로 프론트엔드 변경이 필요 없다.
# Redis 캐싱 + TTL 만료 시 RDB 적재(write-behind) 패턴을 채택하며,
# 이 스키마는 toggle/is_liked/count 세 API에서 공통으로 사용된다.

class LikeResponse(BaseModel):
    """
    영화 좋아요 응답 (공통).

    Backend monglepick-backend `LikeResponse` record와 완전히 동일한 JSON 구조를
    반환하도록 camelCase alias를 사용한다. 이 덕분에 Nginx 경로 라우팅만으로
    백엔드 → recommend(FastAPI) 이관 시 클라이언트 수정이 불필요하다.

    필드:
    - liked: 현재 사용자의 활성 좋아요 여부 (공개 count API에서는 항상 false 고정)
    - likeCount: 해당 영화의 전체 활성 좋아요 수
    """
    model_config = ConfigDict(populate_by_name=True)

    liked: bool = Field(
        description="현재 사용자의 활성 좋아요 여부",
    )
    like_count: int = Field(
        default=0,
        alias="likeCount",
        description="해당 영화의 전체 활성 좋아요 수",
        ge=0,
        serialization_alias="likeCount",
    )


# =========================================
# 위시리스트 / 리뷰 관련 스키마 (Recommend v2 이관)
# =========================================

class WishlistStatusResponse(BaseModel):
    """현재 영화의 위시리스트 포함 여부 응답"""
    wishlisted: bool = Field(description="현재 사용자의 위시리스트 포함 여부")


class WishlistToggleResponse(BaseModel):
    """위시리스트 추가/삭제 결과 응답"""
    wishlisted: bool = Field(description="처리 후 위시리스트 포함 여부")


class WishlistItem(BaseModel):
    """위시리스트 개별 항목"""
    wishlist_id: int = Field(description="위시리스트 항목 ID")
    movie_id: str = Field(description="영화 ID")
    created_at: datetime = Field(description="위시리스트 추가 시각")
    movie: MovieBrief = Field(description="위시리스트에 담긴 영화 정보")


class WishlistListResponse(BaseModel):
    """위시리스트 목록 응답"""
    wishlist: list[WishlistItem] = Field(description="위시리스트 항목 목록")
    total: int = Field(description="전체 위시리스트 개수")


class FavoriteMovieItem(BaseModel):
    """최애 영화 개별 항목"""
    fav_movie_id: int = Field(description="최애 영화 항목 ID")
    movie_id: str = Field(description="영화 ID")
    priority: int = Field(description="전시 우선순위")
    created_at: datetime | None = Field(default=None, description="등록 시각")
    movie: MovieBrief = Field(description="전시할 영화 정보")


class FavoriteMovieListResponse(BaseModel):
    """사용자의 최애 영화 목록 응답"""
    favorite_movies: list[FavoriteMovieItem] = Field(description="최애 영화 항목 목록")
    total: int = Field(description="현재 저장된 최애 영화 수")
    max_count: int = Field(description="저장 가능한 최대 최애 영화 수")


class FavoriteMovieSaveRequest(BaseModel):
    """최애 영화 저장/순서 저장 요청"""
    movie_ids: list[str] = Field(
        default_factory=list,
        description="사용자가 선택한 영화 ID 목록 (순서 포함, 최대 9편)",
    )


class FavoriteGenreOption(BaseModel):
    """선호 장르 선택 UI에 노출할 장르 옵션"""
    genre_id: int = Field(description="genre_master.genre_id")
    genre_code: str = Field(description="장르 코드")
    genre_name: str = Field(description="장르명")
    contents_count: int = Field(description="해당 장르의 컨텐츠 수")


class FavoriteGenreItem(BaseModel):
    """사용자가 저장한 선호 장르 항목"""
    fav_genre_id: int = Field(description="선호 장르 항목 ID")
    genre_id: int = Field(description="genre_master.genre_id")
    priority: int = Field(description="저장 순서 기반 우선순위")
    created_at: datetime | None = Field(default=None, description="등록 시각")
    genre: FavoriteGenreOption = Field(description="선택된 장르 정보")


class FavoriteGenreListResponse(BaseModel):
    """선호 장르 설정 화면 초기 로드 응답"""
    available_genres: list[FavoriteGenreOption] = Field(description="선택 가능한 장르 목록")
    selected_genres: list[FavoriteGenreItem] = Field(description="현재 저장된 선호 장르 목록")


class FavoriteGenreSaveRequest(BaseModel):
    """선호 장르 저장 요청"""
    genre_ids: list[int] = Field(
        default_factory=list,
        description="사용자가 선택한 genre_master.genre_id 목록 (순서 포함)",
    )


class ReviewAuthor(BaseModel):
    """리뷰 작성자 표시 정보"""
    nickname: str = Field(description="작성자 닉네임")


class ReviewItem(BaseModel):
    """영화 리뷰 개별 항목"""
    id: int = Field(description="리뷰 ID")
    movie_id: str = Field(description="영화 ID")
    movie_title: str | None = Field(default=None, description="리뷰 대상 영화 제목")
    poster_url: str | None = Field(default=None, description="리뷰 대상 영화 포스터 URL")
    rating: float = Field(description="평점")
    content: str | None = Field(default=None, description="리뷰 본문")
    author: ReviewAuthor = Field(description="작성자 정보")
    is_spoiler: bool = Field(default=False, description="스포일러 포함 여부")
    is_mine: bool = Field(default=False, description="현재 로그인 사용자의 리뷰 여부")
    review_source: str | None = Field(default=None, description="리뷰 작성 출처")
    review_category_code: str | None = Field(default=None, description="리뷰 카테고리 코드")
    created_at: datetime = Field(description="리뷰 작성 시각")
    like_count: int = Field(default=0, description="리뷰 좋아요 수")
    liked: bool = Field(default=False, description="현재 로그인 사용자의 리뷰 좋아요 여부")


class ReviewListResponse(BaseModel):
    """영화별 리뷰 목록 응답"""
    reviews: list[ReviewItem] = Field(description="리뷰 목록")
    total: int = Field(description="전체 리뷰 수")


class UserReviewListResponse(BaseModel):
    """마이페이지용 내 리뷰 목록 응답"""
    reviews: list[ReviewItem] = Field(description="내가 작성한 리뷰 목록")
    pagination: PaginationMeta = Field(description="페이지네이션 정보")


class ReviewCreateRequest(BaseModel):
    """리뷰 작성 요청"""
    movie_id: str | None = Field(default=None, description="리뷰 대상 영화 ID (호환용)")
    rating: float = Field(description="평점", ge=0.5, le=5.0)
    content: str | None = Field(default=None, description="리뷰 본문")
    is_spoiler: bool = Field(default=False, description="스포일러 포함 여부")
    review_source: str | None = Field(default=None, description="리뷰 작성 출처")
    review_category_code: str | None = Field(default=None, description="리뷰 카테고리 코드")


class ReviewUpdateRequest(BaseModel):
    """리뷰 수정 요청"""
    rating: float = Field(description="평점", ge=0.5, le=5.0)
    content: str | None = Field(default=None, description="리뷰 본문")
    is_spoiler: bool = Field(default=False, description="스포일러 포함 여부")


class OnboardingStatusResponse(BaseModel):
    """
    시작 미션 온보딩 상태 확인 응답

    영화 월드컵, 선호 장르, 최애 영화 3개 미션의 완료 여부와 진행 수를 반환합니다.
    """
    is_completed: bool = Field(description="온보딩 전체 완료 여부")
    completed_mission_count: int = Field(description="완료한 미션 수")
    worldcup_completed: bool = Field(description="월드컵 완료 여부")
    favorite_genres_completed: bool = Field(description="선호 장르 저장 완료 여부")
    favorite_movies_completed: bool = Field(description="최애 영화 저장 완료 여부")
    favorite_genre_count: int = Field(description="저장된 선호 장르 개수")
    favorite_movie_count: int = Field(description="저장된 최애 영화 개수")


# =========================================
# OCR 영수증 분석
# =========================================

class OcrAnalyzeRequest(BaseModel):
    """영수증 OCR 분석 요청"""
    image_url: str = Field(description="분석할 영수증 이미지 URL")
    event_id: str | None = Field(default=None, description="OCR 이벤트 ID (로깅용)")


class OcrAnalyzeResponse(BaseModel):
    """영수증 OCR 분석 결과 — 6개 필드 개별 성공 여부 포함.

    응답 JSON 키는 snake_case 그대로 사용한다. Backend `OcrAnalysisClient.OcrResponse`
    가 `@JsonNaming(SnakeCaseStrategy)` 로 record 필드(camelCase) ↔ JSON(snake_case) 매핑을
    수행하므로 여기서 alias 를 camel 로 바꾸면 Backend 역직렬화가 모두 null 로 깨진다.
    """

    success: bool = Field(description="OCR 텍스트 추출 성공 여부 (개별 필드와 독립)")
    # 전체 상태: SUCCESS(영화명+관람일 모두) / PARTIAL_SUCCESS(1개 이상) / FAILED(없음)
    status: str = Field(default="FAILED", description="전체 추출 상태")
    movie_name: str | None = Field(default=None, description="추출된 영화명")
    watch_date: str | None = Field(default=None, description="추출된 관람일 (YYYY-MM-DD)")
    headcount: int | None = Field(default=None, description="추출된 관람 인원 수")
    seat: str | None = Field(default=None, description="추출된 좌석 정보")
    screening_time: str | None = Field(default=None, description="추출된 상영 시간 (HH:MM)")
    theater: str | None = Field(default=None, description="추출된 상영관")
    parsed_text: str | None = Field(default=None, description="OCR 원문 전체 텍스트")
    confidence: float = Field(default=0.0, description="추출 신뢰도 (0.0~1.0)")
    error_message: str | None = Field(default=None, description="오류 메시지 (실패 시)")
    venue: str | None = Field(default=None, description="영화관 지점명 (예: CGV 홍대)")
    watched_at: str | None = Field(default=None, description="관람일시 조합 (YYYY-MM-DD HH:MM)")
    movie_name_ok: bool = Field(default=False, description="영화명 추출 성공 여부")
    watch_date_ok: bool = Field(default=False, description="관람일 추출 성공 여부")
    headcount_ok: bool = Field(default=False, description="인원 수 추출 성공 여부")
    seat_ok: bool = Field(default=False, description="좌석 추출 성공 여부")
    screening_time_ok: bool = Field(default=False, description="상영 시간 추출 성공 여부")
    theater_ok: bool = Field(default=False, description="상영관 추출 성공 여부")
    venue_ok: bool = Field(default=False, description="영화관 지점명 추출 성공 여부")
