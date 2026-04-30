"""
영화 조회/검색 리포지토리 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 MovieRepository를 Raw SQL로 재구현합니다.
aiomysql DictCursor를 사용하여 결과를 딕셔너리로 받고,
MovieDTO로 변환합니다.

성능 최적화:
- LIKE 검색: prefix match 우선 (title LIKE 'keyword%')
- JSON 장르 필터: JSON_CONTAINS 함수 사용
- COUNT 쿼리 분리: 페이지네이션 total 계산을 별도 쿼리로 분리
- 파라미터 바인딩: SQL Injection 방지 (%s 플레이스홀더)
"""

import json
import logging
from typing import Optional

import aiomysql
from pydantic import ValidationError

from app.v2.model.dto import MovieDTO
from app.repository.movie_repository import (
    EXCLUDED_SEARCH_CERTIFICATIONS,
    EXCLUDED_SEARCH_GENRES,
)

logger = logging.getLogger(__name__)


class MovieRepository:
    """영화 테이블 조회 리포지토리 (Raw SQL)"""

    _VALID_POSTER_PATH_SQL = (
        "poster_path IS NOT NULL "
        "AND TRIM(poster_path) <> '' "
        "AND poster_path NOT LIKE 'http://%%' "
        "AND poster_path NOT LIKE 'https://%%' "
        "AND poster_path LIKE '/%%'"
    )

    def __init__(self, conn: aiomysql.Connection):
        """
        Args:
            conn: aiomysql 비동기 커넥션
        """
        self._conn = conn

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
        popularity_min: float | None = None,
        popularity_max: float | None = None,
        vote_count_min: int | None = None,
        sort_by: str = "rating",
        sort_order: str = "desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[MovieDTO], int]:
        """
        영화를 검색하고 필터링된 결과와 전체 건수를 반환합니다.

        Args:
            keyword: 검색 키워드 (제목/감독/배우)
            search_type: 검색 대상 ("title", "director", "actor", "all")
            genre: 장르 필터 (예: "액션")
            year_from: 개봉 연도 시작 (포함)
            year_to: 개봉 연도 끝 (포함)
            rating_min: 최소 평점 (포함)
            rating_max: 최대 평점 (포함)
            popularity_min: 최소 인기도 (포함)
            popularity_max: 최대 인기도 (포함)
            sort_by: 정렬 기준 ("rating", "release_year", "title")
            sort_order: 정렬 방향 ("asc", "desc")
            page: 페이지 번호 (1부터 시작)
            size: 페이지당 항목 수

        Returns:
            (영화 DTO 목록, 전체 건수) 튜플
        """
        # ─────────────────────────────────────
        # WHERE 절 동적 구성
        # ─────────────────────────────────────
        conditions: list[str] = []
        params: list = []

        # 검색 결과 공통 노출 정책을 모든 Raw SQL 경로에 동일하게 적용한다.
        conditions.append("(adult IS NULL OR adult = 0)")
        conditions.append(
            "(certification IS NULL OR certification NOT IN (%s, %s))"
        )
        params.extend(list(EXCLUDED_SEARCH_CERTIFICATIONS))
        conditions.append('(genres IS NULL OR CAST(genres AS CHAR) NOT LIKE %s)')
        params.append(f'%"{EXCLUDED_SEARCH_GENRES[0]}"%')

        # 키워드 검색 필터
        if keyword and keyword.strip():
            keyword_stripped = keyword.strip()
            like_pattern = f"%{keyword_stripped}%"

            if search_type == "all":
                # 통합 검색: 제목 + 감독 + 배우를 OR 조건으로 묶어 검색
                conditions.append(
                    "(title LIKE %s OR title_en LIKE %s "
                    "OR director LIKE %s OR CAST(cast_members AS CHAR) LIKE %s)"
                )
                params.extend([like_pattern, like_pattern, like_pattern, like_pattern])
            elif search_type == "title":
                # 제목 검색: 한국어 제목 + 영어 원제 모두 검색
                conditions.append("(title LIKE %s OR title_en LIKE %s)")
                params.extend([like_pattern, like_pattern])
            elif search_type == "director":
                # 감독 검색
                conditions.append("director LIKE %s")
                params.append(like_pattern)
            elif search_type == "actor":
                # 배우 검색: cast_members JSON 컬럼에서 LIKE로 검색
                conditions.append("cast_members IS NOT NULL AND CAST(cast_members AS CHAR) LIKE %s")
                params.append(like_pattern)

        # 장르 필터 (JSON_CONTAINS 사용)
        if genre:
            conditions.append("JSON_CONTAINS(genres, JSON_QUOTE(%s))")
            params.append(genre)

        if genres:
            unique_genres = list(dict.fromkeys(genres))
            genre_conditions = ["JSON_CONTAINS(genres, JSON_QUOTE(%s))" for _ in unique_genres]
            conditions.append(f"({' OR '.join(genre_conditions)})")
            params.extend(unique_genres)

        # 연도 필터
        if year_from is not None:
            conditions.append("release_year >= %s")
            params.append(year_from)
        if year_to is not None:
            conditions.append("release_year <= %s")
            params.append(year_to)

        # 평점 필터
        if rating_min is not None:
            conditions.append("rating >= %s")
            params.append(rating_min)
        if rating_max is not None:
            conditions.append("rating <= %s")
            params.append(rating_max)
        if popularity_min is not None:
            conditions.append("popularity_score >= %s")
            params.append(popularity_min)
        if popularity_max is not None:
            conditions.append("popularity_score <= %s")
            params.append(popularity_max)
        if vote_count_min is not None:
            conditions.append("vote_count >= %s")
            params.append(vote_count_min)

        # WHERE 절 조합
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # ─────────────────────────────────────
        # 정렬 적용 (NULLS LAST 구현: column IS NULL, column ASC/DESC)
        # ─────────────────────────────────────
        direction = "ASC" if sort_order == "asc" else "DESC"
        sort_parts: list[str] = []
        sort_params: list = []

        if genre_match_groups:
            for alias_group in genre_match_groups:
                unique_aliases = [alias for alias in dict.fromkeys(alias_group) if alias]
                if not unique_aliases:
                    continue
                alias_sql = " OR ".join(["JSON_CONTAINS(genres, JSON_QUOTE(%s))" for _ in unique_aliases])
                sort_parts.append(f"(CASE WHEN ({alias_sql}) THEN 1 ELSE 0 END) DESC")
                sort_params.extend(unique_aliases)

        if sort_by == "title":
            sort_parts.extend([
                f"title IS NULL ASC",
                f"title {direction}",
                "release_year IS NULL ASC",
                "release_year DESC",
            ])
        elif sort_by == "release_date":
            sort_parts.extend([
                "release_year IS NULL ASC",
                f"release_year {direction}",
                "rating IS NULL ASC",
                "rating DESC",
            ])
        else:
            column = "rating"
            sort_parts.extend([
                f"{column} IS NULL ASC",
                f"{column} {direction if sort_by == 'rating' else 'DESC'}",
                "vote_count IS NULL ASC",
                "vote_count DESC",
                "release_year IS NULL ASC",
                "release_year DESC",
            ])

        order_clause = f"ORDER BY {', '.join(sort_parts)}"

        # ─────────────────────────────────────
        # 페이지네이션
        # ─────────────────────────────────────
        offset = (page - 1) * size
        limit_clause = "LIMIT %s OFFSET %s"

        # ─────────────────────────────────────
        # 검색 결과 쿼리 실행
        # ─────────────────────────────────────
        select_sql = f"SELECT * FROM movies {where_clause} {order_clause} {limit_clause}"
        select_params = params + sort_params + [size, offset]

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(select_sql, select_params)
            rows = await cur.fetchall()

        # ─────────────────────────────────────
        # 전체 건수 쿼리 실행 (별도 쿼리로 분리하여 성능 최적화)
        # ─────────────────────────────────────
        count_sql = f"SELECT COUNT(movie_id) AS total FROM movies {where_clause}"
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(count_sql, params)
            count_row = await cur.fetchone()

        total = count_row["total"] if count_row else 0
        movies = [MovieDTO(**row) for row in rows]

        return movies, total

    async def find_by_id(self, movie_id: str) -> MovieDTO | None:
        """
        영화 ID로 단건 조회합니다.

        먼저 movies.movie_id 로 조회하고, 없으면 숫자형 ID에 한해 tmdb_id 로 한 번 더 조회합니다.
        검색 인덱스/외부 링크가 TMDB ID를 들고 들어오는 경우를 흡수하기 위한 fallback 입니다.

        Args:
            movie_id: 영화 고유 ID (VARCHAR(50)) 또는 TMDB ID 문자열

        Returns:
            MovieDTO 또는 None
        """
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
            row = await cur.fetchone()
            if row is None and movie_id.isdigit():
                await cur.execute("SELECT * FROM movies WHERE tmdb_id = %s", (int(movie_id),))
                row = await cur.fetchone()

        if row is None:
            return None

        try:
            return MovieDTO(**row)
        except ValidationError as exc:
            logger.error(
                "movie_detail_row_validation_failed movie_id=%s errors=%s row_excerpt=%s",
                movie_id,
                exc.errors(),
                {
                    "movie_id": row.get("movie_id"),
                    "title": row.get("title"),
                    "release_year": row.get("release_year"),
                    "runtime": row.get("runtime"),
                    "rating": row.get("rating"),
                    "vote_count": row.get("vote_count"),
                    "adult": row.get("adult"),
                    "source": row.get("source"),
                },
            )
            raise

    async def find_by_ids(self, movie_ids: list[str]) -> list[MovieDTO]:
        """
        여러 영화 ID로 일괄 조회합니다.

        Args:
            movie_ids: 영화 ID 목록 (VARCHAR(50))

        Returns:
            MovieDTO 목록
        """
        if not movie_ids:
            return []

        # IN 절 플레이스홀더 동적 생성: (%s, %s, %s, ...)
        placeholders = ", ".join(["%s"] * len(movie_ids))
        sql = f"SELECT * FROM movies WHERE movie_id IN ({placeholders})"

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, movie_ids)
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_by_identifiers(self, identifiers: list[str]) -> list[MovieDTO]:
        """
        다양한 외부 식별자로 영화를 일괄 조회합니다.

        Qdrant/Neo4j 후보는 `tmdb_id`, `kobis_movie_cd`, `imdb_id`, `kmdb_id`, `movie_id` 중
        어느 값을 들고 올지 일정하지 않으므로, 다섯 필드를 모두 대상으로 매칭한다.

        Args:
            identifiers: 외부 식별자 문자열 목록

        Returns:
            MovieDTO 목록
        """
        normalized_ids = [str(identifier).strip() for identifier in identifiers if str(identifier).strip()]
        if not normalized_ids:
            return []

        unique_ids = list(dict.fromkeys(normalized_ids))
        placeholders = ", ".join(["%s"] * len(unique_ids))
        sql = (
            "SELECT * FROM movies "
            f"WHERE movie_id IN ({placeholders}) "
            f"OR CAST(tmdb_id AS CHAR) IN ({placeholders}) "
            f"OR imdb_id IN ({placeholders}) "
            f"OR kobis_movie_cd IN ({placeholders}) "
            f"OR kmdb_id IN ({placeholders})"
        )
        params = unique_ids * 5

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_home_box_office_source_movies(self, limit: int = 120) -> list[MovieDTO]:
        """
        홈 인기 영화용 박스오피스 원본 후보를 최신 날짜 우선순으로 조회합니다.

        최신 target_dt부터 과거로 내려가며 같은 movie_id는 한 번만 남기고,
        이후 서비스 레이어가 재개봉/중복 적재 보강을 적용할 수 있도록 movies 원본 레코드를 반환합니다.
        """
        normalized_limit = max(1, min(limit, 500))
        sql = """
            WITH matched_box_office AS (
                SELECT
                    m.movie_id AS movie_id,
                    b.target_dt AS target_dt,
                    b.rank_no AS rank_no,
                    b.audi_cnt AS audi_cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.movie_id
                        ORDER BY b.target_dt DESC, COALESCE(b.rank_no, 999999) ASC, b.audi_cnt DESC, m.movie_id ASC
                    ) AS movie_occurrence_rank
                FROM box_office_daily b
                JOIN movies m
                  ON (
                    (b.movie_id IS NOT NULL AND b.movie_id = m.movie_id)
                    OR (b.movie_id IS NULL AND b.movie_cd = m.kobis_movie_cd)
                  )
            ),
            deduped_box_office AS (
                SELECT movie_id, target_dt, rank_no, audi_cnt
                FROM matched_box_office
                WHERE movie_occurrence_rank = 1
            )
            SELECT m.*
            FROM deduped_box_office d
            JOIN movies m ON m.movie_id = d.movie_id
            ORDER BY
                d.target_dt DESC,
                COALESCE(d.rank_no, 999999) ASC,
                d.audi_cnt DESC,
                d.movie_id ASC
            LIMIT %s
        """

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (normalized_limit,))
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_with_posters_by_title(
        self,
        *,
        title: str | None,
        title_en: str | None = None,
        limit: int = 20,
    ) -> list[MovieDTO]:
        """
        제목 기준으로 포스터가 있는 후보 영화를 조회합니다.

        홈 박스오피스에서 재개봉/중복 적재 레코드의 식별자 매칭이 실패했을 때만
        사용하는 2차 fallback 입니다. 동명 영화 오탐을 줄이기 위해 부분 일치가 아닌
        한/영 제목 exact match만 허용합니다.
        """
        title_candidates = []
        for value in (title, title_en):
            normalized = value.strip() if value and value.strip() else None
            if normalized and normalized not in title_candidates:
                title_candidates.append(normalized)

        if not title_candidates:
            return []

        normalized_limit = max(1, min(limit, 50))
        conditions: list[str] = []
        params: list[object] = []

        for candidate in title_candidates:
            conditions.append("title = %s")
            params.append(candidate)
            conditions.append("title_en = %s")
            params.append(candidate)

        sql = (
            "SELECT * FROM movies "
            f"WHERE {self._VALID_POSTER_PATH_SQL} "
            f"AND ({' OR '.join(conditions)}) "
            "ORDER BY "
            "vote_count DESC, "
            "rating DESC, "
            "release_year DESC "
            "LIMIT %s"
        )
        params.append(normalized_limit)

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_with_posters_by_titles(
        self,
        titles: list[str],
        limit: int = 200,
    ) -> list[MovieDTO]:
        """
        여러 제목 exact match 기준으로 포스터가 있는 후보 영화를 일괄 조회합니다.

        홈 박스오피스 보강 시 title fallback을 배치 처리하여 N+1 쿼리를 방지합니다.
        """
        normalized_titles = [
            title.strip()
            for title in titles
            if isinstance(title, str) and title.strip()
        ]
        unique_titles = list(dict.fromkeys(normalized_titles))
        if not unique_titles:
            return []

        normalized_limit = max(1, min(limit, 500))
        placeholders = ", ".join(["%s"] * len(unique_titles))
        sql = (
            "SELECT * FROM movies "
            f"WHERE {self._VALID_POSTER_PATH_SQL} "
            f"AND (title IN ({placeholders}) OR title_en IN ({placeholders})) "
            "ORDER BY "
            "vote_count DESC, "
            "rating DESC, "
            "release_year DESC "
            "LIMIT %s"
        )
        params = unique_titles + unique_titles + [normalized_limit]

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_by_collection_name(
        self,
        collection_name: str,
        *,
        exclude_movie_id: str | None = None,
    ) -> list[MovieDTO]:
        """
        동일 컬렉션에 속한 영화를 모두 조회합니다.

        Args:
            collection_name: movies.collection_name 값
            exclude_movie_id: 제외할 기준 영화 ID

        Returns:
            MovieDTO 목록
        """
        normalized_collection_name = collection_name.strip()
        if not normalized_collection_name:
            return []

        sql = (
            "SELECT * FROM movies "
            "WHERE collection_name = %s "
        )
        params: list[object] = [normalized_collection_name]

        if exclude_movie_id:
            sql += "AND movie_id <> %s "
            params.append(exclude_movie_id)

        sql += (
            "ORDER BY "
            "release_year IS NULL ASC, "
            "release_year ASC, "
            "vote_count DESC, "
            "rating DESC"
        )

        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

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
        prefix_sql = (
            "SELECT title FROM movies "
            "WHERE title LIKE %s "
            "ORDER BY rating IS NULL, rating DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(prefix_sql, (f"{prefix_stripped}%", limit))
            prefix_rows = await cur.fetchall()

        titles = [row["title"] for row in prefix_rows]

        # prefix match가 부족하면 포함 검색 추가
        if len(titles) < limit:
            remaining = limit - len(titles)
            contains_sql = (
                "SELECT title FROM movies "
                "WHERE title LIKE %s AND title NOT LIKE %s "
                "ORDER BY rating IS NULL, rating DESC "
                "LIMIT %s"
            )
            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    contains_sql,
                    (f"%{prefix_stripped}%", f"{prefix_stripped}%", remaining),
                )
                contains_rows = await cur.fetchall()
            titles.extend(row["title"] for row in contains_rows)

        return titles

    async def find_by_genre(
        self,
        genre: str,
        limit: int = 5,
        min_rating: float = 6.0,
    ) -> list[MovieDTO]:
        """
        특정 장르의 대표 영화를 조회합니다.

        온보딩에서 장르별 대표 영화 포스터 표시에 사용합니다.
        평점이 높고 포스터가 있는 영화를 우선 반환합니다.

        Args:
            genre: 장르명
            limit: 최대 반환 건수
            min_rating: 최소 평점 기준

        Returns:
            해당 장르의 대표 영화 DTO 목록
        """
        sql = (
            "SELECT * FROM movies "
            "WHERE JSON_CONTAINS(genres, JSON_QUOTE(%s)) "
            "AND rating >= %s "
            f"AND {self._VALID_POSTER_PATH_SQL} "
            "ORDER BY rating DESC "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (genre, min_rating, limit))
            rows = await cur.fetchall()

        return [MovieDTO(**row) for row in rows]

    async def find_random_by_genres(
        self,
        genres: list[str],
        count: int = 16,
        min_rating: float = 5.0,
    ) -> list[MovieDTO]:
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
            랜덤 선택된 영화 DTO 목록
        """
        # 각 장르에서 균등 분배할 영화 수 계산
        per_genre = max(count // len(genres), 2)
        movies: list[MovieDTO] = []
        seen_ids: set[str] = set()

        for genre in genres:
            # RAND()로 랜덤 선택, 포스터 있는 영화만
            sql = (
                "SELECT * FROM movies "
                "WHERE JSON_CONTAINS(genres, JSON_QUOTE(%s)) "
                "AND rating >= %s "
                f"AND {self._VALID_POSTER_PATH_SQL} "
                "ORDER BY RAND() "
                "LIMIT %s"
            )
            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, (genre, min_rating, per_genre * 2))
                rows = await cur.fetchall()

            for row in rows:
                movie = MovieDTO(**row)
                if movie.movie_id not in seen_ids and len(movies) < count:
                    movies.append(movie)
                    seen_ids.add(movie.movie_id)

        # 부족하면 평점 높은 영화로 보충
        if len(movies) < count:
            remaining = count - len(movies)
            if seen_ids:
                # 이미 선택된 영화 제외
                placeholders = ", ".join(["%s"] * len(seen_ids))
                sql = (
                    f"SELECT * FROM movies "
                    f"WHERE movie_id NOT IN ({placeholders}) "
                    f"AND {self._VALID_POSTER_PATH_SQL} "
                    f"AND rating >= %s "
                    f"ORDER BY rating DESC "
                    f"LIMIT %s"
                )
                params = list(seen_ids) + [min_rating, remaining]
            else:
                sql = (
                    "SELECT * FROM movies "
                    f"WHERE {self._VALID_POSTER_PATH_SQL} "
                    "AND rating >= %s "
                    "ORDER BY rating DESC "
                    "LIMIT %s"
                )
                params = [min_rating, remaining]

            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()

            movies.extend(MovieDTO(**row) for row in rows)

        return movies[:count]

    async def get_all_genres(self) -> list[str]:
        """
        DB에 존재하는 모든 장르를 중복 없이 반환합니다.

        movies 테이블의 genres JSON 컬럼에서 고유 장르를 추출합니다.
        Python에서 JSON 파싱하여 호환성을 보장합니다.

        Returns:
            고유 장르 목록 (정렬됨)
        """
        sql = "SELECT genres FROM movies WHERE genres IS NOT NULL"
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()

        genre_set: set[str] = set()
        for row in rows:
            genres_value = row["genres"]
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
