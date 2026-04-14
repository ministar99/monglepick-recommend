"""
영화 조회/검색 리포지토리

MySQL movies 테이블에 대한 읽기 전용 쿼리를 담당합니다.
DDL 기준: movie_id VARCHAR(50) PK, release_year INT, genres JSON

검색 필터(제목/감독/배우, 장르, 연도, 평점)와
정렬(평점순, 연도순, 제목순)을 조합하여 쿼리를 동적으로 생성합니다.

성능 최적화:
- LIKE 검색: 인덱스 활용을 위해 prefix match 우선 (title LIKE 'keyword%')
- JSON 장르 필터: JSON_CONTAINS 함수 사용
- COUNT 쿼리 분리: 페이지네이션 total 계산을 별도 쿼리로 분리
- director 컬럼 직접 검색: DDL에 director VARCHAR(200) 존재
"""

from sqlalchemy import Select, String, case, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.model.entity import Movie

# 검색 결과에서는 성인물/청소년관람불가 콘텐츠를 공통 제외합니다.
EXCLUDED_SEARCH_CERTIFICATIONS = (
    "청소년 관람 불가",
    "19세관람가(청소년관람불가)",
)
EXCLUDED_SEARCH_GENRES = ("에로",)


class MovieRepository:
    """영화 테이블 조회 리포지토리"""

    def __init__(self, session: AsyncSession):
        """
        Args:
            session: SQLAlchemy 비동기 세션
        """
        self._session = session

    @property
    def _dialect_name(self) -> str:
        """현재 세션이 연결된 DB dialect 이름을 반환합니다."""
        bind = self._session.bind
        return bind.dialect.name if bind is not None else ""

    async def search(
        self,
        keyword: str | None = None,
        search_type: str = "title",
        genre: str | None = None,
        genres: list[str] | None = None,
        genre_match_groups: list[list[str]] | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        rating_min: float | None = None,
        rating_max: float | None = None,
        vote_count_min: int | None = None,
        sort_by: str = "rating",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[Movie], int]:
        """
        영화를 검색하고 필터링된 결과와 전체 건수를 반환합니다.

        Args:
            keyword: 검색 키워드 (제목/감독/배우)
            search_type: 검색 대상 ("title", "director", "actor", "all")
            genre: 장르 필터 (예: "액션")
            genres: 다중 장르 검색용 장르 목록 (OR 조건)
            genre_match_groups: 선택 장르별 alias 그룹 목록 (매칭 개수 우선 정렬용)
            year_from: 개봉 연도 시작 (포함)
            year_to: 개봉 연도 끝 (포함)
            rating_min: 최소 평점 (포함)
            rating_max: 최대 평점 (포함)
            vote_count_min: 최소 평점 참여 인원 수 (포함)
            sort_by: 정렬 기준 ("relevance", "rating", "release_year", "title")
            sort_order: 정렬 방향 ("asc", "desc")
            page: 페이지 번호 (1부터 시작)
            size: 페이지당 항목 수

        Returns:
            (영화 목록, 전체 건수) 튜플
        """
        # 기본 쿼리 구성 (PK: movie_id VARCHAR(50))
        query = select(Movie)
        count_query = select(func.count(Movie.movie_id))
        # 검색 결과 공통 노출 정책을 먼저 적용해 이후 모든 검색 경로에 일관되게 반영합니다.
        visibility_conditions = self._build_search_visibility_conditions()
        for visibility_condition in visibility_conditions:
            query = query.where(visibility_condition)
            count_query = count_query.where(visibility_condition)

        # ─────────────────────────────────────
        # 키워드 검색 필터 적용
        # ─────────────────────────────────────
        if keyword and keyword.strip():
            keyword_stripped = keyword.strip()
            # LIKE 패턴 생성 (양쪽 와일드카드)
            like_pattern = f"%{keyword_stripped}%"
            actor_like = f"%{keyword_stripped}%"

            if search_type == "all":
                # 통합 검색: 제목 + 감독 + 배우를 OR 조건으로 묶어 검색
                all_condition = or_(
                    Movie.title.ilike(like_pattern),
                    Movie.title_en.ilike(like_pattern),
                    Movie.director.ilike(like_pattern),
                    self._json_text_like(Movie.cast_members, actor_like),
                )
                query = query.where(all_condition)
                count_query = count_query.where(all_condition)
                
            elif search_type == "title":
                # 제목 검색: 한국어 제목 + 영어 원제 모두 검색
                query = query.where(
                    (Movie.title.ilike(like_pattern)) |
                    (Movie.title_en.ilike(like_pattern))
                )
                count_query = count_query.where(
                    (Movie.title.ilike(like_pattern)) |
                    (Movie.title_en.ilike(like_pattern))
                )
                
            elif search_type == "director":
                # 감독 검색: DDL에 director VARCHAR(200) 컬럼 존재
                query = query.where(Movie.director.ilike(like_pattern))
                count_query = count_query.where(Movie.director.ilike(like_pattern))
                
            elif search_type == "actor":
                # 배우 검색: cast JSON 컬럼에서 LIKE로 검색
                query = query.where(Movie.cast_members.isnot(None))
                query = query.where(self._json_text_like(Movie.cast_members, actor_like))
                count_query = count_query.where(Movie.cast_members.isnot(None))
                count_query = count_query.where(self._json_text_like(Movie.cast_members, actor_like))

        # ─────────────────────────────────────
        # 장르 필터 (JSON_CONTAINS 사용)
        # ─────────────────────────────────────
        if genre:
            genre_condition = self._json_array_contains(Movie.genres, genre)
            query = query.where(genre_condition)
            count_query = count_query.where(genre_condition)

        # ─────────────────────────────────────
        # 다중 장르 필터 (선택 장르 중 하나 이상 포함)
        # ─────────────────────────────────────
        if genres:
            genre_conditions = [
                self._json_array_contains(Movie.genres, genre_name)
                for genre_name in dict.fromkeys(genres)
            ]
            if genre_conditions:
                multi_genre_condition = or_(*genre_conditions)
                query = query.where(multi_genre_condition)
                count_query = count_query.where(multi_genre_condition)

        # ─────────────────────────────────────
        # 연도 필터 (release_year INT 컬럼 직접 비교)
        # ─────────────────────────────────────
        if year_from is not None:
            query = query.where(Movie.release_year >= year_from)
            count_query = count_query.where(Movie.release_year >= year_from)

        if year_to is not None:
            query = query.where(Movie.release_year <= year_to)
            count_query = count_query.where(Movie.release_year <= year_to)

        # ─────────────────────────────────────
        # 평점 필터
        # ─────────────────────────────────────
        if rating_min is not None:
            query = query.where(Movie.rating >= rating_min)
            count_query = count_query.where(Movie.rating >= rating_min)

        if rating_max is not None:
            query = query.where(Movie.rating <= rating_max)
            count_query = count_query.where(Movie.rating <= rating_max)

        # ─────────────────────────────────────
        # 평점 참여 인원 수 필터
        # ─────────────────────────────────────
        if vote_count_min is not None:
            query = query.where(Movie.vote_count >= vote_count_min)
            count_query = count_query.where(Movie.vote_count >= vote_count_min)

        # ─────────────────────────────────────
        # 정렬 적용
        # ─────────────────────────────────────
        if genre_match_groups:
            # 장르 탐색 검색은 "선택 장르를 많이 만족한 영화"를 먼저 보여줍니다.
            query = self._apply_genre_match_priority(
                query=query,
                genre_match_groups=genre_match_groups,
                sort_by=sort_by,
                sort_order=sort_order,
            )
        else:
            query = self._apply_sort(query, sort_by, sort_order)

        # ─────────────────────────────────────
        # 페이지네이션 적용
        # ─────────────────────────────────────
        offset = (page - 1) * size
        query = query.offset(offset).limit(size)

        # ─────────────────────────────────────
        # 쿼리 실행
        # ─────────────────────────────────────
        # 검색 결과 조회
        result = await self._session.execute(query)
        movies = list(result.scalars().all())

        # 전체 건수 조회 (별도 쿼리로 분리하여 성능 최적화)
        count_result = await self._session.execute(count_query)
        total = count_result.scalar() or 0

        return movies, total

    async def find_by_id(self, movie_id: str) -> Movie | None:
        """
        영화 ID로 단건 조회합니다.

        Args:
            movie_id: 영화 고유 ID (VARCHAR(50))

        Returns:
            Movie 엔티티 또는 None
        """
        result = await self._session.execute(
            select(Movie).where(Movie.movie_id == movie_id)
        )
        return result.scalar_one_or_none()

    async def find_by_ids(self, movie_ids: list[str]) -> list[Movie]:
        """
        여러 영화 ID로 일괄 조회합니다.

        Args:
            movie_ids: 영화 ID 목록 (VARCHAR(50))

        Returns:
            Movie 엔티티 목록
        """
        if not movie_ids:
            return []
        result = await self._session.execute(
            select(Movie).where(Movie.movie_id.in_(movie_ids))
        )
        return list(result.scalars().all())

    async def autocomplete_titles(self, prefix: str, limit: int = 10) -> list[str]:
        """
        제목 자동완성 후보를 반환합니다.

        prefix로 시작하는 영화 제목을 우선 검색하고,
        부족하면 prefix를 포함하는 제목도 추가합니다.

        Args:
            prefix: 입력 중인 검색어
            limit: 최대 반환 건수 (기본 10)

        Returns:
            자동완성 제목 후보 리스트
        """
        prefix_stripped = prefix.strip()
        if not prefix_stripped:
            return []

        # 1순위: prefix로 시작하는 제목 (인덱스 활용)
        prefix_query = (
            select(Movie.title)
            .where(Movie.title.ilike(f"{prefix_stripped}%"))
            .order_by(*self._nulls_last_order(Movie.rating, descending=True))
            .limit(limit)
        )
        result = await self._session.execute(prefix_query)
        titles = list(result.scalars().all())

        # prefix match가 부족하면 포함 검색 추가
        if len(titles) < limit:
            remaining = limit - len(titles)
            contains_query = (
                select(Movie.title)
                .where(
                    Movie.title.ilike(f"%{prefix_stripped}%"),
                    ~Movie.title.ilike(f"{prefix_stripped}%"),  # 이미 포함된 것 제외
                )
                .order_by(*self._nulls_last_order(Movie.rating, descending=True))
                .limit(remaining)
            )
            result = await self._session.execute(contains_query)
            titles.extend(result.scalars().all())

        return titles

    async def find_by_genre(
        self,
        genre: str,
        limit: int = 5,
        min_rating: float = 6.0,
    ) -> list[Movie]:
        """
        특정 장르의 대표 영화를 조회합니다.

        온보딩에서 장르별 대표 영화 포스터 표시에 사용합니다.
        평점이 높고 포스터가 있는 영화를 우선 반환합니다.

        Args:
            genre: 장르명
            limit: 최대 반환 건수
            min_rating: 최소 평점 기준

        Returns:
            해당 장르의 대표 영화 목록
        """
        query = (
            select(Movie)
            .where(
                self._json_array_contains(Movie.genres, genre),
                Movie.rating >= min_rating,
                Movie.poster_path.isnot(None),  # 포스터가 있는 영화만
            )
            .order_by(Movie.rating.desc())
            .limit(limit)
        )
        result = await self._session.execute(query)
        return list(result.scalars().all())

    async def find_random_by_genres(
        self,
        genres: list[str],
        count: int = 16,
        min_rating: float = 5.0,
    ) -> list[Movie]:
        """
        지정된 장르에서 랜덤으로 영화를 선택합니다.

        이상형 월드컵 후보 생성에 사용합니다.
        각 장르에서 균등하게 선택하되, 포스터가 있고
        평점이 일정 이상인 영화만 대상으로 합니다.

        Args:
            genres: 장르 목록
            count: 선택할 총 영화 수 (16 또는 32)
            min_rating: 최소 평점

        Returns:
            랜덤 선택된 영화 목록
        """
        # 각 장르에서 균등 분배할 영화 수 계산
        per_genre = max(count // len(genres), 2)
        movies: list[Movie] = []
        seen_ids: set[str] = set()  # 중복 방지 (movie_id VARCHAR(50))

        for genre in genres:
            # RANDOM()으로 랜덤 선택, 포스터 있는 영화만
            # (MySQL: RAND(), SQLite: RANDOM() — func.random()은 양쪽 호환)
            query = (
                select(Movie)
                .where(
                    self._json_array_contains(Movie.genres, genre),
                    Movie.rating >= min_rating,
                    Movie.poster_path.isnot(None),
                )
                .order_by(func.random())
                .limit(per_genre * 2)  # 여유분 확보
            )
            result = await self._session.execute(query)
            genre_movies = list(result.scalars().all())

            for movie in genre_movies:
                if movie.movie_id not in seen_ids and len(movies) < count:
                    movies.append(movie)
                    seen_ids.add(movie.movie_id)

        # 부족하면 평점 높은 영화로 보충
        if len(movies) < count:
            remaining = count - len(movies)
            supplement_query = (
                select(Movie)
                .where(
                    Movie.movie_id.notin_(seen_ids) if seen_ids else True,
                    Movie.poster_path.isnot(None),
                    Movie.rating >= min_rating,
                )
                .order_by(Movie.rating.desc())
                .limit(remaining)
            )
            result = await self._session.execute(supplement_query)
            movies.extend(result.scalars().all())

        return movies[:count]

    async def get_all_genres(self) -> list[str]:
        """
        DB에 존재하는 모든 장르를 중복 없이 반환합니다.

        movies 테이블의 genres JSON 컬럼에서 고유 장르를 추출합니다.
        MySQL과 SQLite 모두 호환되도록 Python에서 JSON 파싱합니다.

        Returns:
            고유 장르 목록 (정렬됨)
        """
        import json

        # 모든 영화의 genres 컬럼을 조회하여 Python에서 고유 장르 추출
        # (MySQL JSON_TABLE 대신 범용 방식 사용 — SQLite 테스트 호환)
        query = select(Movie.genres).where(Movie.genres.isnot(None))
        result = await self._session.execute(query)
        genre_set: set[str] = set()
        for (genres_value,) in result.fetchall():
            # JSON 컬럼: SQLAlchemy가 자동 디시리얼라이즈하면 list, 아니면 str
            if isinstance(genres_value, list):
                genre_set.update(genres_value)
            elif isinstance(genres_value, str):
                try:
                    parsed = json.loads(genres_value)
                    if isinstance(parsed, list):
                        genre_set.update(parsed)
                except (json.JSONDecodeError, TypeError):
                    pass
        return sorted(genre_set)

    def _apply_sort(
        self,
        query: Select,
        sort_by: str,
        sort_order: str,
    ) -> Select:
        """
        정렬 조건을 쿼리에 적용합니다.

        Args:
            query: 기존 쿼리
            sort_by: 정렬 기준 ("relevance", "rating", "release_year", "title")
            sort_order: 정렬 방향 ("asc", "desc")

        Returns:
            정렬이 적용된 쿼리
        """
        # 정렬 컬럼 매핑 (DDL 기준: release_year INT)
        sort_column_map = {
            "relevance": Movie.rating,  # 현재는 별도 relevance score가 없어 평점을 보조 지표로 사용
            "rating": Movie.rating,
            "release_year": Movie.release_year,
            "release_date": Movie.release_year,  # 하위 호환: release_date → release_year
            "title": Movie.title,
        }
        column = sort_column_map.get(sort_by, Movie.rating)

        if sort_order == "asc":
            return query.order_by(*self._nulls_last_order(column, descending=False))
        else:
            return query.order_by(*self._nulls_last_order(column, descending=True))

    def _apply_genre_match_priority(
        self,
        query: Select,
        genre_match_groups: list[list[str]],
        sort_by: str,
        sort_order: str,
    ) -> Select:
        """
        선택 장르 매칭 개수 우선 정렬을 적용합니다.

        예를 들어 사용자가 장르 3개를 골랐다면:
        1. 3개 모두 포함된 영화
        2. 2개 포함된 영화
        3. 1개 포함된 영화
        순서로 먼저 정렬하고, 같은 구간 안에서는 기존 정렬 기준을 유지합니다.
        """
        match_score_terms = []

        for alias_group in genre_match_groups:
            unique_aliases = [alias for alias in dict.fromkeys(alias_group) if alias]
            if not unique_aliases:
                continue

            alias_conditions = [
                self._json_array_contains(Movie.genres, alias)
                for alias in unique_aliases
            ]
            matched_group_condition = (
                alias_conditions[0]
                if len(alias_conditions) == 1
                else or_(*alias_conditions)
            )
            match_score_terms.append(case((matched_group_condition, 1), else_=0))

        if not match_score_terms:
            return self._apply_sort(query, sort_by, sort_order)

        # 선택 장르별로 최대 1점만 더해 실제 선택 장르 충족 개수를 계산합니다.
        match_score = match_score_terms[0]
        for term in match_score_terms[1:]:
            match_score = match_score + term

        prioritized_query = query.order_by(match_score.desc())
        return self._apply_sort(prioritized_query, sort_by, sort_order)

    def _json_text_like(self, column, pattern: str):
        """JSON/배열 컬럼을 문자열로 캐스팅해 LIKE 검색 조건을 생성합니다."""
        return cast(column, String).like(pattern)

    def _json_array_contains(self, column, value: str):
        """
        JSON 배열 포함 여부 조건을 생성합니다.

        MySQL에서는 JSON_CONTAINS를 사용하고, 테스트용 SQLite에서는 문자열 LIKE로 폴백합니다.
        """
        if self._dialect_name == "mysql":
            return func.json_contains(column, func.json_quote(value)) == 1
        return self._json_text_like(column, f'%"{value}"%')

    def _build_search_visibility_conditions(self) -> tuple:
        """
        검색 결과 공통 노출 정책 조건을 반환합니다.

        - 청소년 관람 불가 인증 영화 제외
        - 에로 장르 영화 제외
        """
        certification_visible_condition = or_(
            Movie.certification.is_(None),
            ~Movie.certification.in_(EXCLUDED_SEARCH_CERTIFICATIONS),
        )
        adult_visible_condition = or_(
            Movie.adult.is_(None),
            Movie.adult.is_(False),
        )
        genre_visible_condition = or_(
            Movie.genres.is_(None),
            ~self._json_array_contains(Movie.genres, EXCLUDED_SEARCH_GENRES[0]),
        )
        return (
            adult_visible_condition,
            certification_visible_condition,
            genre_visible_condition,
        )

    def _nulls_last_order(self, column, *, descending: bool):
        """
        NULL 값을 마지막으로 보내는 정렬 절을 반환합니다.

        MySQL은 `NULLS LAST` 구문을 지원하지 않아, `column IS NULL`을 먼저 정렬해
        NULL이 아닌 값을 앞쪽으로 배치합니다.
        """
        direction = column.desc() if descending else column.asc()
        return (column.is_(None), direction)
