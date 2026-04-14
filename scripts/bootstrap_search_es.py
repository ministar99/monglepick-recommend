"""
recommend 전용 Elasticsearch 검색 자산 bootstrap 스크립트.

사용 예시:
    uv run python scripts/bootstrap_search_es.py
    uv run python scripts/bootstrap_search_es.py --mapping-only
    uv run python scripts/bootstrap_search_es.py --reindex-all
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict

from app.search_es_bootstrap import SearchESBootstrapper


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap Elasticsearch search assets for recommend.")
    parser.add_argument(
        "--mapping-only",
        action="store_true",
        help="매핑만 추가하고 기존 문서 backfill은 실행하지 않습니다.",
    )
    parser.add_argument(
        "--reindex-all",
        action="store_true",
        help="전체 문서를 update_by_query로 다시 색인합니다.",
    )
    args = parser.parse_args()

    bootstrapper = SearchESBootstrapper()
    try:
        result = await bootstrapper.bootstrap(
            run_backfill=not args.mapping_only,
            reindex_all=args.reindex_all,
        )
        if result is None:
            print("search_es bootstrap skipped")
            return 1

        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0
    finally:
        await bootstrapper.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
