"""
wikidata_fetcher.py
~~~~~~~~~~~~~~~~~~~
Выгрузка данных из Wikidata SPARQL endpoint с пагинацией,
retry-логикой (tenacity) и rate limiting.

Результат каждого типа запроса сохраняется в отдельный Parquet-файл.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from queries import ALL_QUERIES

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "TVSeriesKnowledgeGraph/1.0 (https://github.com/yourname/repo)"

PAGE_SIZE = 10_000          # строк за один запрос (лимит Wikidata — 10k-50k)
REQUEST_DELAY = 2.0         # секунд между запросами (уважаем rate limit)
MAX_PAGES = 500             # защита от бесконечного цикла (500 × 10k = 5M строк)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("wikidata_fetcher")

# ---------------------------------------------------------------------------
# SPARQL-клиент
# ---------------------------------------------------------------------------
def _make_sparql() -> SPARQLWrapper:
    sparql = SPARQLWrapper(SPARQL_ENDPOINT, agent=USER_AGENT)
    sparql.setReturnFormat(JSON)
    return sparql


# ---------------------------------------------------------------------------
# Retry-обёртка над одним запросом
# ---------------------------------------------------------------------------
@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _execute_query(sparql: SPARQLWrapper, query: str) -> list[dict[str, Any]]:
    """Выполнить один SPARQL-запрос, вернуть список биндингов."""
    sparql.setQuery(query)
    results = sparql.query().convert()
    bindings = results["results"]["bindings"]  # type: ignore[index]
    return bindings


# ---------------------------------------------------------------------------
# Преобразование биндингов Wikidata → плоский dict
# ---------------------------------------------------------------------------
def _flatten(bindings: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    for b in bindings:
        row: dict[str, str] = {}
        for key, val in b.items():
            raw = val["value"]
            # Wikidata URI → Q-идентификатор
            if raw.startswith("http://www.wikidata.org/entity/"):
                raw = raw.split("/")[-1]
            row[key] = raw
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Постраничная выгрузка одного типа данных
# ---------------------------------------------------------------------------
def fetch_query(
    name: str,
    query_template: str,
    page_size: int = PAGE_SIZE,
) -> pd.DataFrame:
    sparql = _make_sparql()
    all_rows: list[dict[str, str]] = []

    log.info("▶ Начинаем выгрузку: %s", name)

    for page in range(MAX_PAGES):
        offset = page * page_size
        query = query_template.format(limit=page_size, offset=offset)

        try:
            bindings = _execute_query(sparql, query)
        except Exception as exc:
            log.error("Ошибка на странице %d запроса '%s': %s", page, name, exc)
            break

        rows = _flatten(bindings)
        all_rows.extend(rows)

        log.info(
            "  %s | страница %d | получено %d строк | итого %d",
            name, page, len(rows), len(all_rows),
        )

        if len(rows) < page_size:
            # Последняя страница — данные закончились
            break

        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(all_rows)
    log.info("✔ %s: всего %d строк", name, len(df))
    return df


# ---------------------------------------------------------------------------
# Сохранение в Parquet
# ---------------------------------------------------------------------------
def save_parquet(df: pd.DataFrame, name: str) -> Path:
    path = OUTPUT_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, compression="zstd")
    log.info("  → сохранено: %s (%.1f MB)", path, path.stat().st_size / 1e6)
    return path


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def run(query_names: list[str] | None = None) -> None:
    """
    Запустить выгрузку для указанных типов запросов.
    Если query_names=None — запускаем все.
    """
    targets = query_names or list(ALL_QUERIES.keys())

    for name in targets:
        template = ALL_QUERIES[name]
        df = fetch_query(name, template)
        if not df.empty:
            save_parquet(df, name)
        else:
            log.warning("⚠ Пустой результат для '%s', Parquet не создан", name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Wikidata ETL — выгрузка в Parquet")
    parser.add_argument(
        "--queries",
        nargs="*",
        choices=list(ALL_QUERIES.keys()),
        help="Какие типы выгружать (по умолчанию — все)",
    )
    args = parser.parse_args()
    run(args.queries)