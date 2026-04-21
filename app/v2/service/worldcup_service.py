"""
이상형 월드컵 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 WorldcupService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직(토너먼트 구성, 선호도 분석 알고리즘)은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import json
import logging
import random
from collections import Counter
from dataclasses import dataclass

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    GenrePreference,
    MovieBrief,
    WorldcupBracketResponse,
    WorldcupCategoryOptionResponse,
    WorldcupGenreOptionResponse,
    WorldcupMatch,
    WorldcupResultResponse,
    WorldcupSelectionRequest,
    WorldcupSelectionResponse,
    WorldcupSourceType,
    WorldcupStartOptionsRequest,
    WorldcupStartOptionsResponse,
    WorldcupStartRequest,
)
from app.v2.model.dto import MovieDTO
from app.v2.repository.movie_repository import MovieRepository
from app.v2.repository.user_preference_repository import UserPreferenceRepository

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라운드별 가중치 (후반 라운드일수록 높은 가중치)
# ─────────────────────────────────────────
ROUND_WEIGHTS: dict[int, float] = {
    64: 1.0,
    32: 1.0,
    16: 1.0,
    8: 2.0,
    4: 3.0,
    2: 4.0,
}
MIN_GENRE_VOTE_COUNT = 100
SUPPORTED_ROUND_SIZES = (64, 32, 16, 8)
EXCLUDED_CUSTOM_GENRES = ("에로", "동성애", "반공/분단", "계몽")


@dataclass(frozen=True)
class CandidatePoolInfo:
    source_type: WorldcupSourceType
    category_id: int | None
    selected_genres: list[str]
    candidate_pool_size: int


class WorldcupService:
    """이상형 월드컵 비즈니스 로직 서비스 (v2 Raw SQL)"""

    # Redis 키 접두어: 월드컵 진행 상태 저장
    REDIS_KEY_PREFIX = "worldcup:"

    def __init__(self, conn: aiomysql.Connection, redis_client: aioredis.Redis):
        """
        Args:
            conn: aiomysql 비동기 커넥션
            redis_client: Redis 비동기 클라이언트
        """
        self._conn = conn
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(conn)
        self._pref_repo = UserPreferenceRepository(conn)

    async def get_available_categories(self) -> list[WorldcupCategoryOptionResponse]:
        """사용자에게 노출할 활성 월드컵 카테고리 목록을 반환합니다."""
        sql = (
            "SELECT category_id, category_code, category_name, description, display_order "
            "FROM worldcup_category "
            "WHERE is_enabled = b'1' "
            "ORDER BY display_order DESC, category_name ASC"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()

        responses: list[WorldcupCategoryOptionResponse] = []
        for row in rows:
            candidate_pool_size = await self._count_active_candidates_by_category(row["category_id"])
            preview_movie_id, preview_poster_url = await self._resolve_category_preview(
                row["category_id"]
            )
            responses.append(
                WorldcupCategoryOptionResponse(
                    categoryId=row["category_id"],
                    categoryCode=row["category_code"],
                    categoryName=row["category_name"],
                    description=row.get("description"),
                    displayOrder=row.get("display_order") or 0,
                    candidatePoolSize=candidate_pool_size,
                    availableRoundSizes=self._compute_available_round_sizes(candidate_pool_size),
                    previewMovieId=preview_movie_id,
                    previewPosterUrl=preview_poster_url,
                    isReady=candidate_pool_size >= 16,
                )
            )
        return responses

    async def get_available_genres(self) -> list[WorldcupGenreOptionResponse]:
        """커스텀 월드컵 빌더용 장르 목록을 반환합니다."""
        placeholders = ", ".join(["%s"] * len(EXCLUDED_CUSTOM_GENRES))
        sql = (
            "SELECT genre_code, genre_name, contents_count "
            "FROM genre_master "
            "WHERE contents_count > %s "
            f"  AND genre_name NOT IN ({placeholders}) "
            "ORDER BY contents_count DESC, genre_name ASC"
        )
        params = (MIN_GENRE_VOTE_COUNT, *EXCLUDED_CUSTOM_GENRES)
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        return [
            WorldcupGenreOptionResponse(
                genreCode=row["genre_code"],
                genreName=row["genre_name"],
                contentsCount=row["contents_count"],
            )
            for row in rows
        ]

    async def _resolve_category_preview(
        self,
        category_id: int,
    ) -> tuple[str | None, str | None]:
        """카테고리 카드에 사용할 대표 포스터 1장을 무작위로 조회합니다."""
        random_func = "RAND()" if self._movie_repo._dialect_name == "mysql" else "RANDOM()"
        sql = (
            "SELECT m.movie_id, m.poster_path "
            "FROM worldcup_candidate wc "
            "JOIN movies m ON m.movie_id = wc.movie_id "
            "WHERE wc.category_id = %s "
            "  AND wc.is_active = %s "
            "  AND m.poster_path IS NOT NULL "
            f"ORDER BY {random_func} "
            "LIMIT 1"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (category_id, True))
            row = await cur.fetchone()

        if not row:
            return None, None

        poster_path = row.get("poster_path")
        poster_url = (
            f"{self._settings.TMDB_IMAGE_BASE_URL}{poster_path}"
            if poster_path
            else None
        )
        return row.get("movie_id"), poster_url

    async def get_start_options(
        self, request: WorldcupStartOptionsRequest
    ) -> WorldcupStartOptionsResponse:
        """시작 조건에 따른 후보 풀 크기와 가능 라운드 목록을 반환합니다."""
        pool_info = await self._resolve_candidate_pool_info(
            request.sourceType,
            request.categoryId,
            request.selectedGenres,
        )
        return WorldcupStartOptionsResponse(
            sourceType=pool_info.source_type,
            categoryId=pool_info.category_id,
            selectedGenres=pool_info.selected_genres,
            candidatePoolSize=pool_info.candidate_pool_size,
            availableRoundSizes=self._compute_available_round_sizes(pool_info.candidate_pool_size),
        )

    async def start_worldcup(
        self, user_id: str, request: WorldcupStartRequest
    ) -> WorldcupBracketResponse:
        """새 월드컵 시작 요청으로 대진표를 생성합니다."""
        round_size = request.roundSize
        if round_size not in SUPPORTED_ROUND_SIZES:
            raise ValueError(f"지원하지 않는 라운드 크기입니다: {round_size}")

        pool_info = await self._resolve_candidate_pool_info(
            request.sourceType,
            request.categoryId,
            request.selectedGenres,
        )
        available_round_sizes = self._compute_available_round_sizes(pool_info.candidate_pool_size)
        if round_size not in available_round_sizes:
            raise ValueError(
                f"선택한 조건으로는 {round_size}강을 시작할 수 없습니다. "
                f"가능 라운드: {available_round_sizes}"
            )

        if pool_info.source_type == WorldcupSourceType.CATEGORY:
            candidate_ids = await self._resolve_category_candidate_movie_ids(
                pool_info.category_id,
                round_size,
            )
        else:
            candidate_ids = await self._resolve_genre_candidate_movie_ids(
                pool_info.selected_genres,
                round_size,
            )

        movies = await self._fetch_movies_by_ids(candidate_ids)
        if len(movies) != round_size:
            raise ValueError(
                f"월드컵 후보 영화 조회가 완전하지 않습니다. 요청={round_size}, 실제={len(movies)}"
            )

        await self._store_worldcup_state(
            user_id=user_id,
            round_size=round_size,
            candidate_ids=candidate_ids,
            source_type=pool_info.source_type,
            category_id=pool_info.category_id,
            selected_genres=pool_info.selected_genres,
            candidate_pool_size=pool_info.candidate_pool_size,
        )
        return self._build_bracket_response(movies, round_size)

    async def submit_round(
        self, user_id: str, request: WorldcupSelectionRequest
    ) -> WorldcupSelectionResponse:
        """
        월드컵 라운드별 선택 결과를 처리합니다.

        각 매치에서 사용자가 선택한 영화 ID를 받아
        다음 라운드 대진표를 생성하거나, 결승이면 결과를 저장합니다.
        """
        selected_ids = request.selections
        current_round = request.round_size

        # Redis에서 진행 상태 조회
        redis_key = f"{self.REDIS_KEY_PREFIX}{user_id}"
        try:
            state = await self._redis.hgetall(redis_key)
        except Exception:
            state = {}

        # 선택 로그 갱신
        selection_log: list[dict] = []
        if state and state.get("selection_log"):
            try:
                selection_log = json.loads(state["selection_log"])
            except (json.JSONDecodeError, TypeError):
                selection_log = []

        selection_log.append({
            "round": current_round,
            "selected_movie_ids": selected_ids,
        })

        # 결승전인 경우: 결과 저장
        if request.is_final or len(selected_ids) == 1:
            winner_id = selected_ids[0]

            # 준우승 영화 ID 추출
            runner_up_id = None
            if len(selected_ids) == 1 and current_round == 2:
                if state and state.get("candidates"):
                    try:
                        prev_candidates = json.loads(state["candidates"])
                        runner_up_id = next(
                            (cid for cid in prev_candidates if cid != winner_id),
                            None,
                        )
                    except (json.JSONDecodeError, TypeError):
                        pass

            # 선호도 분석
            genre_prefs = await self._analyze_preferences(selection_log)

            # DB에 월드컵 결과 저장
            original_round = int(state.get("round_size", current_round)) if state else current_round
            await self._pref_repo.save_worldcup_result(
                user_id=user_id,
                round_size=original_round,
                winner_movie_id=winner_id,
                runner_up_movie_id=runner_up_id,
                semi_final_movie_ids=None,
                selection_log={"rounds": selection_log},
                genre_preferences=genre_prefs,
            )

            # 분석된 장르 선호도를 user_preferences에도 반영
            top_genres = sorted(genre_prefs, key=genre_prefs.get, reverse=True)[:5]
            await self._pref_repo.save_genres(user_id, top_genres)

            # Redis 상태 정리
            try:
                await self._redis.delete(redis_key)
            except Exception:
                pass

            logger.info(
                f"월드컵 완료: user_id={user_id}, winner={winner_id}, "
                f"genres={top_genres}"
            )

            return WorldcupSelectionResponse(
                message="이상형 월드컵이 완료되었습니다! 결과 분석 페이지로 이동하세요.",
                next_round=None,
                next_matches=None,
            )

        # 다음 라운드 대진표 생성
        next_round = current_round // 2
        selected_movies = await self._movie_repo.find_by_ids(selected_ids)

        movie_dict = {m.movie_id: m for m in selected_movies}
        ordered_movies = [movie_dict[mid] for mid in selected_ids if mid in movie_dict]

        next_matches: list[WorldcupMatch] = []
        for i in range(0, len(ordered_movies), 2):
            if i + 1 < len(ordered_movies):
                match = WorldcupMatch(
                    match_id=i // 2 + 1,
                    movie_a=self._to_movie_brief(ordered_movies[i]),
                    movie_b=self._to_movie_brief(ordered_movies[i + 1]),
                )
                next_matches.append(match)

        # Redis 상태 갱신
        try:
            updated_state = {
                "candidates": json.dumps(selected_ids),
                "selection_log": json.dumps(selection_log),
                "current_round": str(next_round),
            }
            await self._redis.hset(redis_key, mapping=updated_state)
            await self._redis.expire(redis_key, 3600)
        except Exception as e:
            logger.warning(f"Redis 월드컵 상태 갱신 실패: {e}")

        return WorldcupSelectionResponse(
            message=f"{next_round}강 대진표가 준비되었습니다.",
            next_round=next_round,
            next_matches=next_matches,
        )

    async def get_result(self, user_id: str) -> WorldcupResultResponse:
        """월드컵 결과를 분석하여 레이더 차트 데이터를 반환합니다."""
        worldcup = await self._pref_repo.get_worldcup_result(user_id)
        if not worldcup:
            raise ValueError("월드컵 결과가 없습니다. 먼저 월드컵을 진행해주세요.")

        winner = await self._movie_repo.find_by_id(worldcup.winner_movie_id)
        if not winner:
            raise ValueError("우승 영화 정보를 찾을 수 없습니다.")

        runner_up = None
        if worldcup.runner_up_movie_id:
            runner_up_dto = await self._movie_repo.find_by_id(worldcup.runner_up_movie_id)
            if runner_up_dto:
                runner_up = self._to_movie_brief(runner_up_dto)

        # 장르 선호도 파싱
        genre_prefs_data: dict[str, float] = {}
        if worldcup.genre_preferences:
            try:
                genre_prefs_data = json.loads(worldcup.genre_preferences)
            except (json.JSONDecodeError, TypeError):
                pass

        genre_preferences = [
            GenrePreference(genre=genre, score=score)
            for genre, score in sorted(
                genre_prefs_data.items(), key=lambda x: x[1], reverse=True
            )
        ]

        top_genres = [gp.genre for gp in genre_preferences[:3]]

        return WorldcupResultResponse(
            winner=self._to_movie_brief(winner),
            runner_up=runner_up,
            genre_preferences=genre_preferences,
            top_genres=top_genres,
        )

    async def _resolve_candidate_pool_info(
        self,
        source_type: WorldcupSourceType,
        category_id: int | None,
        selected_genres: list[str] | None,
    ) -> CandidatePoolInfo:
        if source_type == WorldcupSourceType.CATEGORY:
            if category_id is None:
                raise ValueError("카테고리 기반 월드컵은 categoryId가 필요합니다.")

            sql = (
                "SELECT category_id "
                "FROM worldcup_category "
                "WHERE category_id = %s AND is_enabled = b'1' "
                "LIMIT 1"
            )
            async with self._conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, (category_id,))
                category_row = await cur.fetchone()

            if category_row is None:
                raise ValueError(f"사용 가능한 월드컵 카테고리를 찾을 수 없습니다: {category_id}")

            candidate_pool_size = await self._count_active_candidates_by_category(category_id)
            return CandidatePoolInfo(
                source_type=source_type,
                category_id=category_id,
                selected_genres=[],
                candidate_pool_size=candidate_pool_size,
            )

        if source_type == WorldcupSourceType.GENRE:
            normalized_genres = self._normalize_genres(selected_genres)
            if not normalized_genres:
                raise ValueError("장르 기반 월드컵은 최소 1개 장르를 선택해야 합니다.")

            candidate_pool_size = await self._count_eligible_movies_by_selected_genres(
                normalized_genres
            )
            return CandidatePoolInfo(
                source_type=source_type,
                category_id=None,
                selected_genres=normalized_genres,
                candidate_pool_size=candidate_pool_size,
            )

        raise ValueError(f"지원하지 않는 월드컵 시작 방식입니다: {source_type}")

    async def _count_active_candidates_by_category(self, category_id: int) -> int:
        sql = (
            "SELECT COUNT(*) AS cnt "
            "FROM worldcup_candidate "
            "WHERE category_id = %s AND is_active = b'1'"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (category_id,))
            row = await cur.fetchone()
        return int((row or {}).get("cnt") or 0)

    async def _resolve_category_candidate_movie_ids(
        self,
        category_id: int | None,
        round_size: int,
    ) -> list[str]:
        if category_id is None:
            raise ValueError("카테고리 기반 월드컵은 categoryId가 필요합니다.")

        sql = (
            "SELECT movie_id "
            "FROM worldcup_candidate "
            "WHERE category_id = %s AND is_active = b'1' "
            "ORDER BY RAND() "
            "LIMIT %s"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (category_id, round_size))
            rows = await cur.fetchall()

        movie_ids = [row["movie_id"] for row in rows]
        if len(movie_ids) < round_size:
            raise ValueError(
                f"카테고리 후보 영화가 부족합니다. 요청 라운드={round_size}, 실제 후보 수={len(movie_ids)}"
            )
        return movie_ids

    async def _count_eligible_movies_by_selected_genres(self, genres: list[str]) -> int:
        conditions = [
            "poster_path IS NOT NULL",
            "COALESCE(vote_count, 0) >= %s",
        ]
        params: list = [MIN_GENRE_VOTE_COUNT]
        genre_conditions = ["JSON_CONTAINS(genres, JSON_QUOTE(%s))" for _ in genres]
        conditions.append(f"({' OR '.join(genre_conditions)})")
        params.extend(genres)

        sql = (
            "SELECT COUNT(movie_id) AS cnt "
            "FROM movies "
            f"WHERE {' AND '.join(conditions)}"
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            row = await cur.fetchone()
        return int((row or {}).get("cnt") or 0)

    async def _resolve_genre_candidate_movie_ids(
        self,
        genres: list[str],
        round_size: int,
    ) -> list[str]:
        conditions = [
            "poster_path IS NOT NULL",
            "COALESCE(vote_count, 0) >= %s",
        ]
        params: list = [MIN_GENRE_VOTE_COUNT]
        genre_conditions = ["JSON_CONTAINS(genres, JSON_QUOTE(%s))" for _ in genres]
        conditions.append(f"({' OR '.join(genre_conditions)})")
        params.extend(genres)

        sql = (
            "SELECT * "
            "FROM movies "
            f"WHERE {' AND '.join(conditions)} "
        )
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        eligible_movies = [MovieDTO(**row) for row in rows]
        movie_ids = self._select_prioritized_genre_movie_ids(eligible_movies, genres, round_size)
        if len(movie_ids) < round_size:
            raise ValueError(
                f"장르 조건을 만족하는 후보 영화가 부족합니다. 요청 라운드={round_size}, 실제 후보 수={len(movie_ids)}"
            )
        return movie_ids

    async def _fetch_movies_by_ids(self, movie_ids: list[str]) -> list[MovieDTO]:
        movies = await self._movie_repo.find_by_ids(movie_ids)
        movie_map = {movie.movie_id: movie for movie in movies}
        return [movie_map[movie_id] for movie_id in movie_ids if movie_id in movie_map]

    async def _store_worldcup_state(
        self,
        user_id: str,
        round_size: int,
        candidate_ids: list[str],
        source_type: WorldcupSourceType,
        category_id: int | None,
        selected_genres: list[str],
        candidate_pool_size: int,
    ) -> None:
        state = {
            "round_size": round_size,
            "candidates": json.dumps(candidate_ids),
            "selection_log": json.dumps([]),
            "current_round": round_size,
            "source_type": source_type.value,
            "category_id": category_id or "",
            "selected_genres_json": json.dumps(selected_genres, ensure_ascii=False),
            "candidate_pool_size": candidate_pool_size,
        }
        redis_key = f"{self.REDIS_KEY_PREFIX}{user_id}"
        try:
            await self._redis.hset(redis_key, mapping=state)
            await self._redis.expire(redis_key, 3600)
        except Exception as e:
            logger.warning(f"Redis 월드컵 상태 저장 실패: {e}")

    def _build_bracket_response(
        self,
        movies: list[MovieDTO],
        round_size: int,
    ) -> WorldcupBracketResponse:
        matches: list[WorldcupMatch] = []
        for i in range(0, len(movies), 2):
            match = WorldcupMatch(
                match_id=i // 2 + 1,
                movie_a=self._to_movie_brief(movies[i]),
                movie_b=self._to_movie_brief(movies[i + 1]),
            )
            matches.append(match)

        total_rounds = 0
        current_round = round_size
        while current_round > 1:
            current_round //= 2
            total_rounds += 1

        return WorldcupBracketResponse(
            round_size=round_size,
            matches=matches,
            total_rounds=total_rounds,
        )

    def _compute_available_round_sizes(self, candidate_pool_size: int) -> list[int]:
        return [
            round_size
            for round_size in SUPPORTED_ROUND_SIZES
            if candidate_pool_size >= round_size
        ]

    def _normalize_genres(self, genres: list[str] | None) -> list[str]:
        if not genres:
            return []
        normalized: list[str] = []
        for genre in genres:
            if genre is None:
                continue
            trimmed = genre.strip()
            if trimmed and trimmed not in normalized:
                normalized.append(trimmed)
        return normalized

    def _select_prioritized_genre_movie_ids(
        self,
        movies: list[MovieDTO],
        selected_genres: list[str],
        round_size: int,
    ) -> list[str]:
        """
        선택 장르를 더 많이 만족하는 영화를 우선 선출합니다.
        """
        grouped_movie_ids: dict[int, list[str]] = {}
        selected_genre_set = set(selected_genres)

        for movie in movies:
            match_count = len(selected_genre_set.intersection(movie.get_genres_list()))
            if match_count <= 0:
                continue
            grouped_movie_ids.setdefault(match_count, []).append(movie.movie_id)

        prioritized_ids: list[str] = []
        for match_count in sorted(grouped_movie_ids.keys(), reverse=True):
            group_ids = grouped_movie_ids[match_count]
            random.shuffle(group_ids)
            prioritized_ids.extend(group_ids)
            if len(prioritized_ids) >= round_size:
                break

        return prioritized_ids[:round_size]

    async def _analyze_preferences(
        self, selection_log: list[dict]
    ) -> dict[str, float]:
        """
        월드컵 선택 로그를 분석하여 장르별 선호도를 계산합니다.

        알고리즘 (v1과 동일):
        1. 각 라운드에서 선택된 영화의 장르를 수집
        2. 라운드 가중치 적용 (후반 라운드 = 더 높은 가중치)
        3. 장르별 가중치 합산
        4. 최대값으로 정규화 (0.0 ~ 1.0)
        """
        genre_scores: Counter[str] = Counter()

        for round_data in selection_log:
            round_num = round_data.get("round", 16)
            selected_ids = round_data.get("selected_movie_ids", [])
            weight = ROUND_WEIGHTS.get(round_num, 1.0)

            movies = await self._movie_repo.find_by_ids(selected_ids)
            for movie in movies:
                genres = movie.get_genres_list()
                for genre in genres:
                    genre_scores[genre] += weight

        if not genre_scores:
            return {}

        max_score = max(genre_scores.values())
        if max_score == 0:
            return {}

        return {
            genre: round(score / max_score, 3)
            for genre, score in genre_scores.items()
        }

    def _to_movie_brief(self, movie: MovieDTO) -> MovieBrief:
        """MovieDTO를 MovieBrief 스키마로 변환합니다."""
        poster_url = None
        if movie.poster_path:
            poster_url = f"{self._settings.TMDB_IMAGE_BASE_URL}{movie.poster_path}"

        return MovieBrief(
            movie_id=movie.movie_id,
            title=movie.title,
            title_en=movie.title_en,
            genres=movie.get_genres_list(),
            release_year=movie.release_year,
            rating=movie.rating,
            poster_url=poster_url,
            trailer_url=movie.trailer_url,
            overview=movie.overview,
        )
