"""
v2 패키지 — Raw SQL (MyBatis 스타일) 기반 추천 서비스

기존 v1(SQLAlchemy ORM)과 동일한 기능을 Raw SQL로 재구현합니다.
aiomysql 커넥션 풀을 직접 관리하며, DictCursor로 결과를 딕셔너리로 받습니다.

전환 전략:
1. /api/v2/ 경로로 병렬 운영
2. 테스트 후 /api/v1/ 경로로 교체
3. 검증 완료 후 기존 ORM 코드 제거
"""
