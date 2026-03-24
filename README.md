# monglepick-recommend

몽글픽 영화 검색 및 회원 개인화 초기 설정 FastAPI 서비스.

Spring Boot 백엔드(`monglepick-backend`)와 MySQL DB를 공유하며, 영화 검색(REQ_031~034)과 온보딩 개인화(REQ_016~019) 기능을 제공합니다.

## 실행

```bash
cp .env.example .env   # 환경변수 설정
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

## API 문서

서버 기동 후 http://localhost:8001/docs 에서 Swagger UI를 확인할 수 있습니다.
