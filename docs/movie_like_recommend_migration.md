# 영화 좋아요(Movie Like) — monglepick-recommend(FastAPI) 이관 설계서

- **작성일**: 2026-04-07
- **대상 도메인**: 영화 좋아요 (`likes` 테이블 단건)
- **From**: monglepick-backend (Spring Boot, `domain/movie/LikeController`)
- **To**: monglepick-recommend (FastAPI, `app/v2/api/like.py`)
- **정합성 모델**: 하이브리드 write-behind (Redis 즉시 반영 + 주기적 MySQL flush)

---

## 1. 이관 배경

영화 좋아요는 다음 특성을 가진다.

- **쓰기 빈도 높음**: 스크롤 피드에서 가볍게 토글되는 저가치 이벤트
- **읽기 빈도 더 높음**: 영화 카드마다 카운트가 노출됨
- **원자적 카운터 성격**: 중복 INSERT 방지만 되면 정합성 이슈 없음
- **리워드 연동 없음**: 좋아요 클릭으로 포인트가 지급되지 않음

Spring Boot + JPA로 매번 `SELECT ... COUNT(*) ... INSERT/UPDATE` 를 수행하는 것은
DB 부하가 크고 응답 지연이 발생한다. 이를 완화하기 위해 Redis 캐시에 즉시 반영하고
DB는 주기적 배치 flush로 비동기 수렴시키는 **write-behind** 패턴을 채택한다.

FastAPI(recommend) 서비스는 이미 Redis 클라이언트 싱글턴(`app/core/redis.py`)과
aiomysql 풀(`app/v2/core/database.py`) 인프라를 갖추고 있어 이관 비용이 낮다.

> **담당자 정리 (2026-04-08 재할당)**
> 정한나는 backend에 신규 코드를 작성하지 않고 monglepick-recommend(FastAPI) 전담.
> 좋아요는 backend에서 초기 구현되었으나, 캐싱/빈번 쓰기 특성상 recommend로 이관하는
> 것이 아키텍처적으로 적합하다는 판단으로 2026-04-07 이관 결정.

---

## 2. 범위

### 이관 대상 (이번 PR)
- **Movie Like**: `likes` 테이블 1건 (domain/movie/Like)

### 이관 제외 (backend 유지)
다음 좋아요 도메인은 community / playlist 경계를 가로지르므로 이번 이관에서 제외한다.
- **ReviewLike** (`review_likes`) — 리뷰 도메인 (이민수)
- **PostLike** (`post_like`) — 커뮤니티 도메인 (이민수)
- **CommentLike** (`comment_likes`) — 커뮤니티 도메인 (이민수)
- **PlaylistLike** (`playlist_likes`) — 플레이리스트 도메인 (김민규)

---

## 3. 정합성 모델 — 하이브리드 Write-Behind

### 핵심 원칙
| 항목 | 정합성 | 비고 |
|---|---|---|
| 카운트 (`like_count`) | Redis **INCR/DECR 즉시 반영** | DB는 flush로 수렴. 캐시 미스 시 `SELECT COUNT(*)`로 lazy 리하이드레이션 |
| 사용자 좋아요 여부 | Redis **SADD/SREM 즉시 반영** | DB는 flush로 수렴. init 플래그 기반 lazy 리하이드레이션 |
| 토글 이력 (dirty 큐) | Redis Hash에 기록 | write-behind 스케줄러가 N초 주기로 드레인 |

### 데이터 흐름

```
[사용자 POST /api/v1/movies/{id}/like]
         │
         ▼
  ┌──────────────────┐
  │  Nginx           │  /api/v1/movies/*/like* → recommend:8001
  └──────────────────┘
         │
         ▼
  ┌──────────────────┐
  │ FastAPI recommend│  app/v2/api/like.py
  │   LikeService    │  ─── (1) 사용자 셋 lazy rehydrate ────────▶ MySQL SELECT
  │                  │  ─── (2) count lazy rehydrate  ─────────▶ MySQL COUNT
  │                  │  ─── (3) SISMEMBER 판정
  │                  │  ─── (4) SADD/SREM + INCR/DECR (Redis) ──▶ Redis
  │                  │  ─── (5) HSET like:dirty (op 기록) ──────▶ Redis
  │                  │  ─── (6) liked/likeCount 즉시 응답 ──────▶ Client
  └──────────────────┘

[APScheduler 60초마다]
         │
         ▼
  ┌──────────────────┐
  │ like_flush       │  ─── (1) RENAME like:dirty → processing:{ts}
  │   (background)   │  ─── (2) HGETALL 스냅샷
  │                  │  ─── (3) batch_apply_toggles
  │                  │  ─── (4) MySQL INSERT/UPDATE(soft-delete 토글)
  │                  │  ─── (5) DEL processing key
  └──────────────────┘
```

### Redis 키 스키마

| 키 | 타입 | TTL | 설명 |
|---|---|---|---|
| `like:count:{movie_id}` | String(int) | 없음 | 영화별 활성 좋아요 수 캐시 (장기) |
| `like:user:{user_id}` | Set&lt;str&gt; | 1시간 | 사용자가 좋아요한 영화 ID 집합 |
| `like:user:{user_id}:init` | String("1") | 1시간 | 사용자 셋 로드 완료 플래그 |
| `like:dirty` | Hash | 없음 | flush 대기 큐. field=`{user_id}\|{movie_id}`, value=JSON `{op, ts}` |
| `like:dirty:processing:{ts}` | Hash | 없음 | 드레인된 스냅샷 (flush 중에만 존재) |

### Dirty 큐 dedup 동작
동일 `(user_id, movie_id)` 쌍에 대해 빠르게 연속 토글(LIKE → UNLIKE → LIKE)되면,
Redis Hash의 field 덮어쓰기 특성으로 **최종 op만 남는다**. 즉 flush 배치는 항상
"최종 상태"만 반영하며, 중간 상태는 실제 응답으로만 반영되고 DB에는 기록되지 않는다.
이 설계는 의도적이다 — 좋아요 자체가 가치가 낮은 이벤트이고, 사용자 관점에서
중요한 것은 "지금 상태"이지 "클릭 히스토리"가 아니기 때문.

### 다중 replica 안전성
FastAPI recommend가 여러 replica로 배포되어 각 인스턴스에 APScheduler가 돌더라도,
`RENAME like:dirty → processing:{ts}`는 Redis 레벨에서 원자적이므로 **한 replica만
실제 flush를 수행**한다. 두 번째 RENAME은 `no such key` 에러로 즉시 no-op 종료.

---

## 4. 장애 처리

### Redis 장애
`LikeService.toggle_like` 내부에서 `redis.asyncio.RedisError` catch → **DB 동기 폴백**으로
전환한다. 이 경로는 dirty 큐를 거치지 않고 `LikeRepository.apply_toggle`을 직접 호출하여
트랜잭션 커밋 시점에 DB가 곧바로 진실 원본이 된다. Redis 복구 후 다음 요청부터는
캐시가 자동 lazy 리하이드레이션된다.

### DB flush 실패
`like_flush.flush_like_dirty_queue`가 DB 반영 중 예외를 던지면, processing key의
엔트리를 원래 dirty 큐로 **복구(HSET)**한다. 다음 주기에 자동 재시도되므로
데이터 손실 창이 flush 주기(기본 60초) 내로 제한된다.

### 데이터 손실 범위
Redis가 **flush 직전에 재시작**되면, 마지막 flush 이후 ~60초간의 토글 이력이
손실될 수 있다. 단 카운트는 DB COUNT로 lazy 재구성되므로 시간이 지나면 수렴한다.
"좋아요는 최대 60초 오차 내에서 정확" 이라는 것이 본 설계의 SLA.

---

## 5. Nginx 라우팅 설정

운영/스테이징 환경의 Nginx 설정에 다음 location 블록을 추가한다 (기존 backend
프록시보다 **앞**에 배치해야 한다).

```nginx
# /Users/yoonhyungjoo/Documents/monglepick/nginx/sites-enabled/monglepick.conf 등

upstream recommend_upstream {
    server 10.20.0.11:8001;        # VM2 — recommend FastAPI
    # 다중 replica 시 여기에 추가
    keepalive 16;
}

upstream backend_upstream {
    server 10.20.0.11:8080;        # VM2 — Spring Boot (backend)
    keepalive 16;
}

server {
    listen 443 ssl http2;
    server_name monglepick.com;

    # ─────────────────────────────────────────────
    # 2026-04-07 이관: 영화 좋아요 → recommend FastAPI
    # 반드시 /api/v1/ 일반 프록시 location 보다 위에 배치
    # 정규식: /api/v1/movies/{id}/like 와 /api/v1/movies/{id}/like/count
    # ─────────────────────────────────────────────
    location ~ ^/api/v1/movies/[^/]+/like(/count)?$ {
        proxy_pass http://recommend_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Authorization $http_authorization;
        # recommend FastAPI는 /api/v2/ prefix를 내부 경로로 사용하지만,
        # Nginx가 URL 재작성 없이 그대로 전달하면 경로 매칭 실패.
        # 따라서 경로 재작성 필요.
        rewrite ^/api/v1/(.*)$ /api/v2/$1 break;
        proxy_pass http://recommend_upstream;
    }

    # 기존 백엔드 프록시 (위 규칙에 걸리지 않은 나머지)
    location /api/ {
        proxy_pass http://backend_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Authorization $http_authorization;
    }

    # 정적 파일 등 기타 location...
}
```

### 적용 절차
```bash
# VM1에서
sudo nginx -t                       # 설정 문법 검증
sudo systemctl reload nginx         # graceful reload
```

### 검증
```bash
# 1. 카운트 API (공개, JWT 불필요)
curl -i https://monglepick.com/api/v1/movies/550/like/count
# 기대: 200 OK, { "liked": false, "likeCount": N }

# 2. 토글 API (JWT 필요)
curl -i -X POST \
     -H "Authorization: Bearer {token}" \
     https://monglepick.com/api/v1/movies/550/like
# 기대: 200 OK, { "liked": true/false, "likeCount": N }
```

### 롤백
Nginx location 블록을 주석 처리하고 `nginx -t && systemctl reload nginx`.
backend의 `LikeController`가 `@Deprecated`만 달린 상태로 유지되므로 즉시 이전
동작으로 복귀 가능하다.

---

## 6. 설정값

`monglepick-recommend/.env` (또는 운영 secret):

```bash
# 좋아요 write-behind flush 주기 (초). 기본 60초.
# 낮추면 데이터 손실 창이 줄지만 DB 부하 증가
LIKE_FLUSH_INTERVAL_SECONDS=60

# flush 활성화 여부. 테스트/점검 시 False로 설정하여 잡 비활성화 가능
LIKE_FLUSH_ENABLED=true

# 한 번의 flush 배치에서 최대 처리 가능한 dirty 엔트리 수.
# 초과분은 다음 주기로 이월된다 (순환 보호)
LIKE_FLUSH_BATCH_MAX=1000
```

---

## 7. 모니터링 포인트

| 지표 | 임계치 | 알람 |
|---|---|---|
| `like:dirty` Hash field 수 | > 5,000 | flush가 못 따라가는 중 — batch_max 상향 or interval 단축 |
| `like:dirty:processing:*` 개수 | > 0 for 5분+ | 이전 flush가 DB 실패로 복구되지 않음 — 로그 확인 |
| `LikeService` 로그 `"Redis 장애 → DB 폴백"` | 지속 발생 | Redis 인스턴스 헬스체크 |
| `like_flush` 로그 `status=error` | 5분 이내 재발 | 수동 개입 필요 |

`app/background/like_flush.py` 의 `logger.info("[like-flush] 완료: drained=%d applied=%d ...")` 라인을
Grafana/Loki로 수집하여 flush 성공률 대시보드를 구성한다.

---

## 8. 이관 체크리스트

### 코드 변경 (완료)
- [x] `app/v2/model/dto.py` — LikeDTO 추가
- [x] `app/model/schema.py` — LikeResponse Pydantic 모델 (camelCase alias)
- [x] `app/v2/repository/like_repository.py` — Raw SQL 리포지토리
- [x] `app/v2/service/like_service.py` — 하이브리드 캐시 서비스
- [x] `app/v2/api/like.py` — 라우터
- [x] `app/v2/api/router.py` — like_router 등록
- [x] `app/core/scheduler.py` — AsyncIOScheduler 싱글턴
- [x] `app/background/like_flush.py` — write-behind flush 잡
- [x] `app/main.py` — lifespan에 scheduler 연동
- [x] `app/config.py` — LIKE_FLUSH_* 설정 추가
- [x] `pyproject.toml` / `requirements.txt` — apscheduler 의존성
- [x] `tests/test_like.py` — 15개 단위 테스트
- [x] `tests/conftest.py` — FakeRedis Set/Hash/INCR/Rename 확장
- [x] Backend Like 관련 5개 클래스에 `@Deprecated` + 주석

### 운영 배포
- [ ] recommend Docker 이미지 재빌드 + deploy
- [ ] Nginx 설정 추가 (`/api/v1/movies/{id}/like*` 프록시)
- [ ] `nginx -t && systemctl reload nginx`
- [ ] curl 수동 검증 (count, toggle)
- [ ] Grafana 대시보드 신규 패널 추가 (`like:dirty` 큐 크기)
- [ ] 한 주 관찰 후 완전 이관 여부 결정

### 롤백 조건
- Redis 장애 빈도 > 주 1회
- flush 실패율 > 1%
- 사용자 좋아요 불일치 민원 발생

발생 시 Nginx 블록 주석 처리로 즉시 backend 경로 복구.

---

## 9. 관련 파일

### recommend (FastAPI)
- `monglepick-recommend/app/v2/api/like.py`
- `monglepick-recommend/app/v2/service/like_service.py`
- `monglepick-recommend/app/v2/repository/like_repository.py`
- `monglepick-recommend/app/v2/model/dto.py` (LikeDTO)
- `monglepick-recommend/app/model/schema.py` (LikeResponse)
- `monglepick-recommend/app/background/like_flush.py`
- `monglepick-recommend/app/core/scheduler.py`
- `monglepick-recommend/app/config.py` (LIKE_FLUSH_*)
- `monglepick-recommend/tests/test_like.py`

### backend (Spring Boot, Deprecated)
- `monglepick-backend/.../domain/movie/entity/Like.java` (DDL 마스터 유지)
- `monglepick-backend/.../domain/movie/repository/LikeRepository.java`
- `monglepick-backend/.../domain/movie/service/LikeService.java`
- `monglepick-backend/.../domain/movie/controller/LikeController.java`
- `monglepick-backend/.../domain/movie/dto/LikeResponse.java`
