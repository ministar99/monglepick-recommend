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

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.model.entity import Movie
from app.model.schema import (
    GenrePreference,
    MovieBrief,
    WorldcupBracketResponse,
    WorldcupMatch,
    WorldcupResultResponse,
    WorldcupSelectionRequest,
    WorldcupSelectionResponse,
)
from app.repository.movie_repository import MovieRepository
from app.repository.user_preference_repository import UserPreferenceRepository

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 라운드별 가중치 (후반 라운드일수록 높은 가중치)
# 16강=1, 8강=2, 4강=3, 결승=4
# ─────────────────────────────────────────
ROUND_WEIGHTS: dict[int, float] = {
    32: 1.0,
    16: 1.0,
    8: 2.0,
    4: 3.0,
    2: 4.0,
}


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
        self._pref_repo = UserPreferenceRepository(session)

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

        Raises:
            ValueError: round_size가 16 또는 32가 아닌 경우
        """
        if round_size not in (16, 32):
            raise ValueError(f"라운드 크기는 16 또는 32만 가능합니다: {round_size}")

        # ─────────────────────────────────────
        # 사용자의 선호 장르 조회 (1단계에서 저장한 것)
        # ─────────────────────────────────────
        pref = await self._pref_repo.get_by_user_id(user_id)
        if pref and pref.preferred_genres:
            # JSON 컬럼: SQLAlchemy가 자동으로 파이썬 리스트로 디시리얼라이즈
            if isinstance(pref.preferred_genres, list):
                selected_genres = pref.preferred_genres
            else:
                try:
                    selected_genres = json.loads(pref.preferred_genres)
                except (json.JSONDecodeError, TypeError):
                    selected_genres = ["액션", "드라마", "코미디"]  # 폴백 장르
        else:
            # 장르 선택을 건너뛴 경우 기본 장르 사용
            selected_genres = ["액션", "드라마", "코미디"]

        # ─────────────────────────────────────
        # 영화 후보 랜덤 선택
        # ─────────────────────────────────────
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
            # 2의 거듭제곱으로 내림 (16, 8, 4, ...)
            actual_size = 1
            while actual_size * 2 <= len(movies):
                actual_size *= 2
            movies = movies[:actual_size]
            round_size = actual_size

        # ─────────────────────────────────────
        # 대진표 매치 구성 (2개씩 짝지어 매치 생성)
        # ─────────────────────────────────────
        matches: list[WorldcupMatch] = []
        for i in range(0, len(movies), 2):
            match = WorldcupMatch(
                match_id=i // 2 + 1,
                movie_a=self._to_movie_brief(movies[i]),
                movie_b=self._to_movie_brief(movies[i + 1]),
            )
            matches.append(match)

        # ─────────────────────────────────────
        # Redis에 월드컵 진행 상태 저장 (TTL 1시간)
        # ─────────────────────────────────────
        state = {
            "round_size": round_size,
            "candidates": json.dumps([m.movie_id for m in movies]),
            "selection_log": json.dumps([]),
            "current_round": round_size,
        }
        redis_key = f"{self.REDIS_KEY_PREFIX}{user_id}"
        try:
            await self._redis.hset(redis_key, mapping=state)
            await self._redis.expire(redis_key, 3600)  # 1시간 TTL
        except Exception as e:
            logger.warning(f"Redis 월드컵 상태 저장 실패: {e}")

        # 총 라운드 수 계산 (예: 16강 → 4라운드)
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
            overview=movie.overview,
        )
