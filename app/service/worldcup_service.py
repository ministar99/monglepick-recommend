"""
이상형 월드컵 서비스

REQ_017: 영화 이상형 월드컵 (16강/32강 토너먼트)
REQ_018: 월드컵 결과 → 장르 선호도 분석 (레이더 차트)

월드컵 흐름:
1. 후보 생성: 사용자가 선택한 장르에서 16/32개 영화를 랜덤 선택
2. 대진표 구성: 영화를 2개씩 매치로 구성
3. 라운드 진행: 매 라운드마다 사용자가 선택한 영화를 서버에 전송
4. 결과 분석: 선택된 영화들의 장르 분포를 분석하여 레이더 차트 데이터 생성

토너먼트 구조 (16강 기준):
- 16강: 8매치 → 승자 8명
- 8강: 4매치 → 승자 4명
- 4강: 2매치 → 승자 2명
- 결승: 1매치 → 우승 1명

선호도 분석 알고리즘:
- 각 라운드에서 선택된 영화의 장르에 가중치 부여
- 후반 라운드(4강, 결승)에서 선택된 장르에 더 높은 가중치
- 가중치: 16강=1, 8강=2, 4강=3, 결승=4
- 정규화하여 0.0~1.0 범위의 레이더 차트 데이터 생성
"""

import json
import logging
from collections import Counter
from dataclasses import dataclass

from sqlalchemy import bindparam, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.entity import Movie, WorldcupCandidate, WorldcupCategory, WorldcupMatch as WorldcupMatchEntity
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
from app.repository.movie_repository import MovieRepository
from app.repository.worldcup_match_repository import WorldcupMatchRepository
from app.repository.user_preference_repository import UserPreferenceRepository
from app.repository.worldcup_session_repository import WorldcupSessionRepository

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라운드별 가중치 (후반 라운드일수록 높은 가중치)
# 16강=1, 8강=2, 4강=3, 결승=4
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
EXCLUDED_CUSTOM_GENRES = ("에로", "동성애", "반공/분단", "계몽", "코메디")


@dataclass(frozen=True)
class CandidatePoolInfo:
    source_type: WorldcupSourceType
    category_id: int | None
    selected_genres: list[str]
    candidate_pool_size: int


class WorldcupService:
    """이상형 월드컵 비즈니스 로직 서비스"""

    # Redis 키 접두어: 월드컵 진행 상태 저장
    REDIS_KEY_PREFIX = "worldcup:"

    def __init__(
        self, session: AsyncSession, redis_client: aioredis.Redis
    ):
        """
        Args:
            session: SQLAlchemy 비동기 세션
            redis_client: Redis 비동기 클라이언트
        """
        self._session = session
        self._redis = redis_client
        self._settings = get_settings()
        self._movie_repo = MovieRepository(session)
        self._match_repo = WorldcupMatchRepository(session)
        self._pref_repo = UserPreferenceRepository(session)
        self._session_repo = WorldcupSessionRepository(session)

    async def get_available_categories(self) -> list[WorldcupCategoryOptionResponse]:
        """사용자에게 노출할 활성 월드컵 카테고리 목록을 반환합니다."""
        result = await self._session.execute(
            select(WorldcupCategory)
            .where(WorldcupCategory.is_enabled.is_(True))
            .order_by(WorldcupCategory.display_order.desc(), WorldcupCategory.category_name.asc())
        )
        categories = list(result.scalars().all())

        responses: list[WorldcupCategoryOptionResponse] = []
        for category in categories:
            candidate_pool_size = await self._count_active_candidates_by_category(category.category_id)
            preview_movie_id, preview_poster_url = await self._resolve_category_preview(
                category.category_id
            )
            responses.append(
                WorldcupCategoryOptionResponse(
                    categoryId=category.category_id,
                    categoryCode=category.category_code,
                    categoryName=category.category_name,
                    description=category.description,
                    displayOrder=category.display_order,
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
        stmt = text(
            """
            SELECT genre_code, genre_name, contents_count
            FROM genre_master
            WHERE contents_count > :minimum_count
              AND genre_name NOT IN :excluded_names
            ORDER BY contents_count DESC, genre_name ASC
            """
        ).bindparams(
            bindparam("minimum_count", value=MIN_GENRE_VOTE_COUNT),
            bindparam("excluded_names", value=list(EXCLUDED_CUSTOM_GENRES), expanding=True),
        )
        result = await self._session.execute(stmt)
        rows = result.mappings().all()
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
        random_func = self._random_function()
        result = await self._session.execute(
            select(Movie.movie_id, Movie.poster_path)
            .join(WorldcupCandidate, WorldcupCandidate.movie_id == Movie.movie_id)
            .where(
                WorldcupCandidate.category_id == category_id,
                WorldcupCandidate.is_active.is_(True),
                Movie.poster_path.isnot(None),
            )
            .order_by(random_func)
            .limit(1)
        )
        row = result.first()
        if row is None:
            return None, None

        movie_id, poster_path = row
        poster_url = (
            f"{self._settings.TMDB_IMAGE_BASE_URL}{poster_path}"
            if poster_path
            else None
        )
        return movie_id, poster_url

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

        await self._session_repo.abandon_in_progress_sessions(user_id)
        worldcup_session = await self._session_repo.create_session(
            user_id=user_id,
            source_type=pool_info.source_type,
            category_id=pool_info.category_id,
            selected_genres=pool_info.selected_genres,
            candidate_pool_size=pool_info.candidate_pool_size,
            round_size=round_size,
        )
        match_rows = await self._match_repo.create_matches(
            session_id=worldcup_session.session_id,
            round_number=round_size,
            candidate_ids=candidate_ids,
        )
        await self._store_worldcup_state(
            user_id=user_id,
            session_id=worldcup_session.session_id,
            round_size=round_size,
            candidate_ids=candidate_ids,
            source_type=pool_info.source_type,
            category_id=pool_info.category_id,
            selected_genres=pool_info.selected_genres,
            candidate_pool_size=pool_info.candidate_pool_size,
        )
        return self._build_bracket_response_from_records(match_rows, movies, round_size)

    async def submit_round(
        self, user_id: str, request: WorldcupSelectionRequest
    ) -> WorldcupSelectionResponse:
        """
        월드컵 라운드별 선택 결과를 처리합니다.

        각 매치에서 사용자가 선택한 영화 ID를 받아
        다음 라운드 대진표를 생성하거나, 결승이면 결과를 저장합니다.

        Args:
            user_id: 사용자 ID
            request: 라운드 선택 결과 (선택한 영화 ID 목록)

        Returns:
            WorldcupSelectionResponse: 다음 라운드 매치 또는 완료 메시지
        """
        selected_ids = request.selections
        current_round = request.round_size

        # ─────────────────────────────────────
        # Redis에서 진행 상태 조회
        # ─────────────────────────────────────
        redis_key = f"{self.REDIS_KEY_PREFIX}{user_id}"
        try:
            state = await self._redis.hgetall(redis_key)
        except Exception:
            state = {}
        session_id = self._parse_session_id(state)
        if session_id is not None:
            await self._match_repo.select_round_winners(session_id, current_round, selected_ids)

        # 선택 로그 갱신
        selection_log: list[dict] = []
        if state and state.get("selection_log"):
            try:
                selection_log = json.loads(state["selection_log"])
            except (json.JSONDecodeError, TypeError):
                selection_log = []

        # 현재 라운드 선택 기록 추가
        selection_log.append({
            "round": current_round,
            "selected_movie_ids": selected_ids,
        })

        # ─────────────────────────────────────
        # 결승전인 경우: 결과 저장
        # ─────────────────────────────────────
        if request.is_final or len(selected_ids) == 1:
            winner_id = selected_ids[0]

            # 준우승 영화 ID 추출 (직전 라운드에서 떨어진 영화)
            runner_up_id = None
            if len(selected_ids) == 1 and current_round == 2:
                # 결승전: 선택되지 않은 영화가 준우승
                if state and state.get("candidates"):
                    try:
                        prev_candidates = json.loads(state["candidates"])
                        # 마지막 라운드의 후보 중 선택되지 않은 것
                        runner_up_id = next(
                            (cid for cid in prev_candidates if cid != winner_id),
                            None,
                        )
                    except (json.JSONDecodeError, TypeError):
                        pass

            # 선호도 분석 실행
            genre_prefs = await self._analyze_preferences(selection_log)

            # DB에 월드컵 결과 저장
            original_round = int(state.get("round_size", current_round)) if state else current_round
            await self._pref_repo.save_worldcup_result(
                user_id=user_id,
                round_size=original_round,
                winner_movie_id=winner_id,
                runner_up_movie_id=runner_up_id,
                semi_final_movie_ids=None,  # 4강 정보는 selection_log에 포함
                selection_log={"rounds": selection_log},
                genre_preferences=genre_prefs,
                session_id=session_id,
            )
            if session_id is not None:
                await self._session_repo.complete_session(session_id, winner_id)

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

        # ─────────────────────────────────────
        # 다음 라운드 대진표 생성
        # ─────────────────────────────────────
        next_round = current_round // 2

        # 선택된 영화들을 조회하여 다음 라운드 매치 구성
        selected_movies = await self._movie_repo.find_by_ids(selected_ids)

        # ID 순서를 유지하기 위해 딕셔너리로 변환 (movie_id VARCHAR(50))
        movie_dict = {m.movie_id: m for m in selected_movies}
        ordered_movies = [movie_dict[mid] for mid in selected_ids if mid in movie_dict]

        # 매치 구성
        next_match_rows: list[WorldcupMatchEntity] = []
        if session_id is not None:
            next_match_rows = await self._match_repo.create_matches(
                session_id=session_id,
                round_number=next_round,
                candidate_ids=selected_ids,
            )
            next_matches = self._to_match_responses(next_match_rows, ordered_movies)
        else:
            next_matches = self._build_match_pairs_without_persistence(ordered_movies)

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
        if session_id is not None:
            await self._session_repo.advance_round(session_id, next_round)

        return WorldcupSelectionResponse(
            message=f"{next_round}강 대진표가 준비되었습니다.",
            next_round=next_round,
            next_matches=next_matches,
        )

    async def get_result(self, user_id: str) -> WorldcupResultResponse:
        """
        월드컵 결과를 분석하여 레이더 차트 데이터를 반환합니다.

        우승/준우승 영화 정보와 장르별 선호도 점수를 포함합니다.

        Args:
            user_id: 사용자 ID

        Returns:
            WorldcupResultResponse: 결과 분석 (레이더 차트 데이터)

        Raises:
            ValueError: 월드컵 결과가 없는 경우
        """
        # DB에서 월드컵 결과 조회
        worldcup = await self._pref_repo.get_worldcup_result(user_id)
        if not worldcup:
            raise ValueError("월드컵 결과가 없습니다. 먼저 월드컵을 진행해주세요.")

        # 우승 영화 조회
        winner = await self._movie_repo.find_by_id(worldcup.winner_movie_id)
        if not winner:
            raise ValueError("우승 영화 정보를 찾을 수 없습니다.")

        # 준우승 영화 조회
        runner_up = None
        if worldcup.runner_up_movie_id:
            runner_up_entity = await self._movie_repo.find_by_id(
                worldcup.runner_up_movie_id
            )
            if runner_up_entity:
                runner_up = self._to_movie_brief(runner_up_entity)

        # 장르 선호도 파싱
        genre_prefs_data: dict[str, float] = {}
        if worldcup.genre_preferences:
            try:
                genre_prefs_data = json.loads(worldcup.genre_preferences)
            except (json.JSONDecodeError, TypeError):
                pass

        # 레이더 차트 데이터 생성
        genre_preferences = [
            GenrePreference(genre=genre, score=score)
            for genre, score in sorted(
                genre_prefs_data.items(), key=lambda x: x[1], reverse=True
            )
        ]

        # 상위 3개 장르
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
        """시작 방식에 맞는 후보 풀 정보를 계산합니다."""
        if source_type == WorldcupSourceType.CATEGORY:
            if category_id is None:
                raise ValueError("카테고리 기반 월드컵은 categoryId가 필요합니다.")

            result = await self._session.execute(
                select(WorldcupCategory).where(
                    WorldcupCategory.category_id == category_id,
                    WorldcupCategory.is_enabled.is_(True),
                )
            )
            category = result.scalar_one_or_none()
            if category is None:
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
        result = await self._session.execute(
            select(func.count(WorldcupCandidate.id)).where(
                WorldcupCandidate.category_id == category_id,
                WorldcupCandidate.is_active.is_(True),
            )
        )
        return int(result.scalar() or 0)

    async def _resolve_category_candidate_movie_ids(
        self,
        category_id: int | None,
        round_size: int,
    ) -> list[str]:
        if category_id is None:
            raise ValueError("카테고리 기반 월드컵은 categoryId가 필요합니다.")

        random_func = self._random_function()
        result = await self._session.execute(
            select(WorldcupCandidate.movie_id)
            .where(
                WorldcupCandidate.category_id == category_id,
                WorldcupCandidate.is_active.is_(True),
            )
            .order_by(random_func)
            .limit(round_size)
        )
        movie_ids = list(result.scalars().all())
        if len(movie_ids) < round_size:
            raise ValueError(
                f"카테고리 후보 영화가 부족합니다. 요청 라운드={round_size}, 실제 후보 수={len(movie_ids)}"
            )
        return movie_ids

    async def _count_eligible_movies_by_selected_genres(self, genres: list[str]) -> int:
        genre_conditions = [
            self._movie_repo._json_array_contains(Movie.genres, genre)
            for genre in genres
        ]
        query = select(func.count(Movie.movie_id)).where(
            Movie.poster_path.isnot(None),
            func.coalesce(Movie.vote_count, 0) >= MIN_GENRE_VOTE_COUNT,
            or_(*genre_conditions),
        )
        result = await self._session.execute(query)
        return int(result.scalar() or 0)

    async def _resolve_genre_candidate_movie_ids(
        self,
        genres: list[str],
        round_size: int,
    ) -> list[str]:
        genre_conditions = [
            self._movie_repo._json_array_contains(Movie.genres, genre)
            for genre in genres
        ]
        query = select(Movie).where(
            Movie.poster_path.isnot(None),
            func.coalesce(Movie.vote_count, 0) >= MIN_GENRE_VOTE_COUNT,
            or_(*genre_conditions),
        )
        result = await self._session.execute(query)
        eligible_movies = list(result.scalars().all())
        movie_ids = self._select_prioritized_genre_movie_ids(eligible_movies, genres, round_size)
        if len(movie_ids) < round_size:
            raise ValueError(
                f"장르 조건을 만족하는 후보 영화가 부족합니다. 요청 라운드={round_size}, 실제 후보 수={len(movie_ids)}"
            )
        return movie_ids

    async def _fetch_movies_by_ids(self, movie_ids: list[str]) -> list[Movie]:
        movies = await self._movie_repo.find_by_ids(movie_ids)
        movie_map = {movie.movie_id: movie for movie in movies}
        return [movie_map[movie_id] for movie_id in movie_ids if movie_id in movie_map]

    async def _store_worldcup_state(
        self,
        user_id: str,
        session_id: int,
        round_size: int,
        candidate_ids: list[str],
        source_type: WorldcupSourceType,
        category_id: int | None,
        selected_genres: list[str],
        candidate_pool_size: int,
    ) -> None:
        state = {
            "session_id": session_id,
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

    def _parse_session_id(self, state: dict[str, str]) -> int | None:
        """Redis 상태에서 session_id를 안전하게 추출합니다."""
        raw_session_id = state.get("session_id") if state else None
        if raw_session_id in (None, ""):
            return None
        try:
            return int(raw_session_id)
        except (TypeError, ValueError):
            logger.warning("유효하지 않은 worldcup session_id 상태값: %s", raw_session_id)
            return None

    def _build_bracket_response(
        self,
        movies: list[Movie],
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

    def _build_bracket_response_from_records(
        self,
        match_rows: list[WorldcupMatchEntity],
        movies: list[Movie],
        round_size: int,
    ) -> WorldcupBracketResponse:
        """저장된 매치 row 기준으로 브래킷 응답을 구성합니다."""
        matches = self._to_match_responses(match_rows, movies)
        return WorldcupBracketResponse(
            round_size=round_size,
            matches=matches,
            total_rounds=self._compute_total_rounds(round_size),
        )

    def _compute_available_round_sizes(self, candidate_pool_size: int) -> list[int]:
        return [
            round_size
            for round_size in SUPPORTED_ROUND_SIZES
            if candidate_pool_size >= round_size
        ]

    def _compute_total_rounds(self, round_size: int) -> int:
        total_rounds = 0
        current_round = round_size
        while current_round > 1:
            current_round //= 2
            total_rounds += 1
        return total_rounds

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

    def _random_function(self):
        return func.rand() if self._movie_repo._dialect_name == "mysql" else func.random()

    def _to_match_responses(
        self,
        match_rows: list[WorldcupMatchEntity],
        movies: list[Movie],
    ) -> list[WorldcupMatch]:
        """DB 매치 row와 영화 엔티티를 API 응답용 매치로 변환합니다."""
        movie_map = {movie.movie_id: movie for movie in movies}
        matches: list[WorldcupMatch] = []
        for row in sorted(match_rows, key=lambda item: item.match_order):
            movie_a = movie_map.get(row.movie_a_id)
            movie_b = movie_map.get(row.movie_b_id)
            if not movie_a or not movie_b:
                continue
            matches.append(
                WorldcupMatch(
                    match_id=row.match_id,
                    movie_a=self._to_movie_brief(movie_a),
                    movie_b=self._to_movie_brief(movie_b),
                )
            )
        return matches

    def _build_match_pairs_without_persistence(
        self,
        movies: list[Movie],
    ) -> list[WorldcupMatch]:
        """세션 ID가 없을 때만 사용하는 임시 매치 응답 생성기."""
        matches: list[WorldcupMatch] = []
        for i in range(0, len(movies), 2):
            if i + 1 < len(movies):
                matches.append(
                    WorldcupMatch(
                        match_id=i // 2 + 1,
                        movie_a=self._to_movie_brief(movies[i]),
                        movie_b=self._to_movie_brief(movies[i + 1]),
                    )
                )
        return matches

    def _select_prioritized_genre_movie_ids(
        self,
        movies: list[Movie],
        selected_genres: list[str],
        round_size: int,
    ) -> list[str]:
        """
        선택 장르를 더 많이 만족하는 영화를 우선 선출합니다.

        예: [로맨스, 액션, 판타지] 선택 시
        - 3개 모두 포함 영화
        - 2개 포함 영화
        - 1개 포함 영화
        순서로 후보를 채웁니다.
        같은 장르 만족 수 그룹 안에서는 movies.popularity_score가 높은 영화를 먼저 선발합니다.
        """
        grouped_movies: dict[int, list[Movie]] = {}
        selected_genre_set = set(selected_genres)

        for movie in movies:
            match_count = len(selected_genre_set.intersection(movie.get_genres_list()))
            if match_count <= 0:
                continue
            grouped_movies.setdefault(match_count, []).append(movie)

        prioritized_ids: list[str] = []
        for match_count in sorted(grouped_movies.keys(), reverse=True):
            sorted_group = sorted(
                grouped_movies[match_count],
                key=lambda movie: (
                    -(movie.popularity_score or 0.0),
                    movie.movie_id,
                ),
            )
            prioritized_ids.extend(movie.movie_id for movie in sorted_group)
            if len(prioritized_ids) >= round_size:
                break

        return prioritized_ids[:round_size]

    async def _analyze_preferences(
        self, selection_log: list[dict]
    ) -> dict[str, float]:
        """
        월드컵 선택 로그를 분석하여 장르별 선호도를 계산합니다.

        알고리즘:
        1. 각 라운드에서 선택된 영화의 장르를 수집
        2. 라운드 가중치 적용 (후반 라운드 = 더 높은 가중치)
        3. 장르별 가중치 합산
        4. 최대값으로 정규화 (0.0 ~ 1.0)

        Args:
            selection_log: 라운드별 선택 기록
                [{"round": 16, "selected_movie_ids": [1, 3, 5, ...]}, ...]

        Returns:
            장르별 선호도 점수 딕셔너리 (예: {"액션": 0.85, "드라마": 0.6})
        """
        # 장르별 가중치 합산 카운터
        genre_scores: Counter[str] = Counter()

        for round_data in selection_log:
            round_num = round_data.get("round", 16)
            selected_ids = round_data.get("selected_movie_ids", [])
            weight = ROUND_WEIGHTS.get(round_num, 1.0)

            # 선택된 영화들의 장르 조회
            movies = await self._movie_repo.find_by_ids(selected_ids)
            for movie in movies:
                genres = movie.get_genres_list()
                for genre in genres:
                    # 라운드 가중치를 적용하여 장르 점수 누적
                    genre_scores[genre] += weight

        # 정규화: 최대값을 1.0으로
        if not genre_scores:
            return {}

        max_score = max(genre_scores.values())
        if max_score == 0:
            return {}

        normalized = {
            genre: round(score / max_score, 3)
            for genre, score in genre_scores.items()
        }
        return normalized

    def _to_movie_brief(self, movie: Movie) -> MovieBrief:
        """Movie 엔티티를 MovieBrief 스키마로 변환합니다."""
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
