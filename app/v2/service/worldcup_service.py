"""
이상형 월드컵 서비스 (v2 Raw SQL)

v1(SQLAlchemy ORM)의 WorldcupService를 Raw SQL 리포지토리 기반으로 재구현합니다.
비즈니스 로직(토너먼트 구성, 선호도 분석 알고리즘)은 v1과 완전히 동일합니다.

변경점: AsyncSession → aiomysql.Connection
"""

import json
import logging
from collections import Counter

import aiomysql
import redis.asyncio as aioredis

from app.config import get_settings
from app.model.schema import (
    GenrePreference,
    MovieBrief,
    WorldcupBracketResponse,
    WorldcupMatch,
    WorldcupResultResponse,
    WorldcupSelectionRequest,
    WorldcupSelectionResponse,
)
from app.v2.model.dto import MovieDTO
from app.v2.repository.movie_repository import MovieRepository
from app.v2.repository.user_preference_repository import UserPreferenceRepository

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라운드별 가중치 (후반 라운드일수록 높은 가중치)
# ─────────────────────────────────────────
ROUND_WEIGHTS: dict[int, float] = {
    32: 1.0,
    16: 1.0,
    8: 2.0,
    4: 3.0,
    2: 4.0,
}


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

    async def generate_bracket(
        self, user_id: str, round_size: int = 16
    ) -> WorldcupBracketResponse:
        """
        월드컵 대진표를 생성합니다.

        사용자가 1단계에서 선택한 장르를 기반으로
        영화 후보를 랜덤 선택하고 대진표를 구성합니다.

        Args:
            user_id: 사용자 ID
            round_size: 라운드 크기 (16 또는 32)

        Returns:
            WorldcupBracketResponse: 대진표 (매치 목록)
        """
        if round_size not in (16, 32):
            raise ValueError(f"라운드 크기는 16 또는 32만 가능합니다: {round_size}")

        # 사용자의 선호 장르 조회
        pref = await self._pref_repo.get_by_user_id(user_id)
        if pref and pref.preferred_genres:
            genres_raw = pref.preferred_genres
            if isinstance(genres_raw, list):
                selected_genres = genres_raw
            elif isinstance(genres_raw, str):
                try:
                    selected_genres = json.loads(genres_raw)
                except (json.JSONDecodeError, TypeError):
                    selected_genres = ["액션", "드라마", "코미디"]
            else:
                selected_genres = ["액션", "드라마", "코미디"]
        else:
            selected_genres = ["액션", "드라마", "코미디"]

        # 영화 후보 랜덤 선택
        movies = await self._movie_repo.find_random_by_genres(
            genres=selected_genres,
            count=round_size,
            min_rating=5.0,
        )

        if len(movies) < round_size:
            logger.warning(
                f"월드컵 후보 부족: 요청={round_size}, 실제={len(movies)}. "
                f"가용 영화 수로 조정합니다."
            )
            actual_size = 1
            while actual_size * 2 <= len(movies):
                actual_size *= 2
            movies = movies[:actual_size]
            round_size = actual_size

        # 대진표 매치 구성
        matches: list[WorldcupMatch] = []
        for i in range(0, len(movies), 2):
            match = WorldcupMatch(
                match_id=i // 2 + 1,
                movie_a=self._to_movie_brief(movies[i]),
                movie_b=self._to_movie_brief(movies[i + 1]),
            )
            matches.append(match)

        # Redis에 월드컵 진행 상태 저장 (TTL 1시간)
        state = {
            "round_size": round_size,
            "candidates": json.dumps([m.movie_id for m in movies]),
            "selection_log": json.dumps([]),
            "current_round": round_size,
        }
        redis_key = f"{self.REDIS_KEY_PREFIX}{user_id}"
        try:
            await self._redis.hset(redis_key, mapping=state)
            await self._redis.expire(redis_key, 3600)
        except Exception as e:
            logger.warning(f"Redis 월드컵 상태 저장 실패: {e}")

        # 총 라운드 수 계산
        total_rounds = 0
        r = round_size
        while r > 1:
            r //= 2
            total_rounds += 1

        return WorldcupBracketResponse(
            round_size=round_size,
            matches=matches,
            total_rounds=total_rounds,
        )

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
