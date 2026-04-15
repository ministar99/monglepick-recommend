# monglepick-recommend

몽글픽 영화 검색 및 회원 개인화 초기 설정 FastAPI 서비스.

Spring Boot 백엔드(`monglepick-backend`)와 MySQL DB를 공유하며, 영화 검색(REQ_031~034)과 온보딩 개인화(REQ_016~019) 기능을 제공합니다.

## 실행

```bash
cp .env.example .env          # 환경변수 설정
uv sync --frozen              # uv.lock 기반 의존성 설치 (.venv 자동 생성)
uv run uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

> 패키지 매니저는 **uv 전용** 입니다. `requirements.txt` 는 사용하지 않으며, 의존성 추가/변경은 `pyproject.toml` + `uv.lock` 으로만 관리합니다.

## API 문서

서버 기동 후 http://localhost:8001/docs 에서 Swagger UI를 확인할 수 있습니다.
