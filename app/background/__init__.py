"""
백그라운드 작업 패키지
=================================================================

FastAPI 애플리케이션 lifespan 동안 주기/지연 실행되는 백그라운드 잡을 모은 곳.

모듈 목록 (2026-04-07 기준):
  - like_flush: 영화 좋아요 write-behind flush
                Redis `like:dirty` 큐 → MySQL `likes` 테이블 배치 반영

향후 추가 예정:
  - trending_persist: Redis Sorted Set 인기 검색어 → MySQL trending_keywords 주기 스냅샷
  - search_history_cleanup: 사용자별 최근 검색어 20건 초과분 주기 삭제 (이미 SearchHistoryRepository에서 처리 중이지만 백업용)
"""
