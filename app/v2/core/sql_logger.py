"""
v2 Raw SQL 쿼리 로깅 (aiomysql Cursor 서브클래스)
=================================================================

v2 는 SQLAlchemy 를 사용하지 않고 aiomysql 로 Raw SQL 을 실행하므로
SQLAlchemy 의 `echo=True` 같은 내장 옵션이 없다. 대신 Cursor 의
`execute()` / `executemany()` 를 override 한 서브클래스를 제공하여
쿼리 문자열과 바인딩 파라미터를 logger 로 출력한다.

적용 전략 (install_sql_logging):
  1. `aiomysql.create_pool(cursorclass=LoggingDictCursor)` 로 pool 기본 cursor 교체
     → 레포지토리에서 `self._conn.cursor()` 무인자 호출 시 로깅 cursor 가 반환됨.
  2. `aiomysql.DictCursor` / `aiomysql.Cursor` 모듈 속성 자체를 로깅 서브클래스로
     교체 → 기존 레포지토리 30+ 곳에 산재한
     `self._conn.cursor(aiomysql.DictCursor)` 명시적 호출도 그대로 로깅됨.
     (레포지토리 파일을 일괄 수정하지 않아도 됨)

주의:
  - 이 모듈은 `init_pool()` 시점(FastAPI lifespan startup)에 한 번만 활성화된다.
  - 운영에서는 `SQL_ECHO=false` 유지 필수. 다음 이유로 운영 활성화 금지:
      * 모든 SQL 이 디스크/로그 집계로 흘러가 성능 저하
      * 바인딩 파라미터(예: user_id, 민감 필드)가 평문 노출
  - `pymysql.cursors` 내부에서 aiomysql.Cursor/DictCursor 를 직접 참조하는 경우는
    없어 보이지만, 만약 향후 깨진다면 monkey-patch 대신 레포지토리 일괄 교체로
    롤백한다.
"""

from __future__ import annotations

import logging
from typing import Optional

import aiomysql

logger = logging.getLogger("monglepick.recommend.sql")


# ─────────────────────────────────────────────────────────
# 내부 믹스인 — execute/executemany 를 가로채 SQL 로그 출력
# ─────────────────────────────────────────────────────────
class _SqlLogMixin:
    """
    aiomysql Cursor 계열에 끼워 넣는 로깅 믹스인.

    SQL 실행 직전에 query 문과 args 를 DEBUG 레벨로 남긴다.
    예외가 발생해도 원본 동작은 그대로 전파하며, 로그만 추가된다.
    """

    async def execute(self, query, args=None):  # type: ignore[override]
        # 파라미터가 있으면 함께 기록 (한 줄로 가독성 유지)
        if args is None:
            logger.debug("SQL: %s", query)
        else:
            logger.debug("SQL: %s | args=%r", query, args)
        return await super().execute(query, args)

    async def executemany(self, query, args):  # type: ignore[override]
        # 배치 INSERT/UPDATE 는 args 를 전부 찍으면 로그가 폭발하므로
        # 건수만 기록하고, 첫 번째 row 샘플만 함께 남긴다.
        try:
            count = len(args) if args is not None else 0
        except TypeError:
            count = -1  # 제너레이터 등 len() 불가 케이스
        sample = None
        if args and count > 0:
            try:
                sample = args[0]
            except (TypeError, IndexError):
                sample = None
        logger.debug(
            "SQL (many): %s | batch_size=%d | sample=%r",
            query, count, sample,
        )
        return await super().executemany(query, args)


class LoggingCursor(_SqlLogMixin, aiomysql.Cursor):
    """일반 튜플 반환 Cursor — INSERT/UPDATE 등 결과가 필요 없는 쿼리에 사용."""
    pass


class LoggingDictCursor(_SqlLogMixin, aiomysql.DictCursor):
    """딕셔너리 반환 Cursor — SELECT 결과를 dict 로 받는 대다수 레포지토리에 사용."""
    pass


# ─────────────────────────────────────────────────────────
# monkey-patch 제어 — 최초 install 시점에만 원본 보존 후 교체
# ─────────────────────────────────────────────────────────
_original_dict_cursor: Optional[type] = None
_original_cursor: Optional[type] = None
_installed: bool = False


def install_sql_logging() -> None:
    """
    v2 Raw SQL 쿼리 로깅을 활성화한다.

    아래 두 가지 치환을 동시에 수행한다:
      1. `aiomysql.DictCursor` → `LoggingDictCursor`
      2. `aiomysql.Cursor`     → `LoggingCursor`

    이후 레포지토리에서 `self._conn.cursor(aiomysql.DictCursor)` 같은 명시적
    호출도 자동으로 로깅 서브클래스를 사용하게 된다.

    멱등성: 이미 설치된 경우 추가 작업 없이 즉시 반환한다.
    """
    global _original_dict_cursor, _original_cursor, _installed
    if _installed:
        return

    _original_dict_cursor = aiomysql.DictCursor
    _original_cursor = aiomysql.Cursor

    # 주의: 속성 교체는 aiomysql 모듈 네임스페이스에만 적용된다.
    # `from aiomysql import DictCursor` 로 캡처한 코드가 있다면 영향을 받지 않으나,
    # recommend 프로젝트 내 레포지토리는 전부 `import aiomysql` + 속성 접근 방식이다.
    aiomysql.DictCursor = LoggingDictCursor  # type: ignore[misc]
    aiomysql.Cursor = LoggingCursor  # type: ignore[misc]

    _installed = True
    logger.info(
        "[v2] SQL 로깅 활성화 — aiomysql.DictCursor/Cursor 가 LoggingDictCursor/LoggingCursor 로 교체됨"
    )


def uninstall_sql_logging() -> None:
    """
    monkey-patch 를 원상복구한다. 주로 테스트에서 사용한다.

    `install_sql_logging()` 이 호출된 적이 없으면 no-op.
    """
    global _installed
    if not _installed:
        return
    if _original_dict_cursor is not None:
        aiomysql.DictCursor = _original_dict_cursor  # type: ignore[misc]
    if _original_cursor is not None:
        aiomysql.Cursor = _original_cursor  # type: ignore[misc]
    _installed = False
    logger.info("[v2] SQL 로깅 비활성화 — aiomysql Cursor 원복 완료")
