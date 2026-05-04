"""
relations_fetcher.py
~~~~~~~~~~~~~~~~~~~~
Выгружает связи (cast, creators, directors, genres, countries, languages,
based_on) только для отфильтрованного списка сериалов из top_series_ids.parquet.

Стратегия: разбиваем wikidata_id на батчи по BATCH_SIZE, для каждого батча
делаем SPARQL-запрос с VALUES. Это намного быстрее и чище, чем выгружать
всё подряд и потом фильтровать.

Вход:  data/processed/top_series_ids.parquet  (wikidata_id, title, views_total)
Выход: data/raw/{relation_type}.parquet
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

log = logging.getLogger("relations_fetcher")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "TVSeriesKnowledgeGraph/1.0 (https://github.com/yourname/repo)"

BATCH_SIZE = 150       # ID за один SPARQL-запрос; 500 давало 431 (URL too large), POST + 150 надёжно
REQUEST_DELAY = 2.0    # сек между запросами

INPUT_PATH = Path("data/processed/top_series_ids.parquet")
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Шаблоны запросов с VALUES
# Плейсхолдер {values_block} — список вида:
#   wd:Q1234 wd:Q5678 wd:Q9012 ...
# ---------------------------------------------------------------------------

RELATION_QUERIES: dict[str, str] = {

    "cast": """
SELECT ?series ?person ?personLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P161 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "creators": """
SELECT ?series ?person ?personLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P162 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "directors": """
SELECT ?series ?person ?personLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P57 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "genres": """
SELECT ?series ?genre ?genreLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P136 ?genre .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "countries": """
SELECT ?series ?country ?countryLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P495 ?country .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "languages": """
SELECT ?series ?language ?languageLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P364 ?language .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",

    "based_on": """
SELECT ?series ?work ?workLabel WHERE {{
  VALUES ?series {{ {values_block} }}
  ?series wdt:P144 ?work .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",
}

# ---------------------------------------------------------------------------
# SPARQL-клиент
# ---------------------------------------------------------------------------
def _make_sparql() -> SPARQLWrapper:
    sparql = SPARQLWrapper(SPARQL_ENDPOINT, agent=USER_AGENT)
    sparql.setReturnFormat(JSON)
    return sparql


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=2, min=4, max=120),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _execute(sparql: SPARQLWrapper, query: str) -> list[dict[str, Any]]:
    sparql.setQuery(query)
    sparql.setMethod("POST")   # POST чтобы не упираться в лимит длины URL (431)
    results = sparql.query().convert()
    return results["results"]["bindings"]  # type: ignore[index]


# ---------------------------------------------------------------------------
# Flatten биндингов Wikidata → плоский dict
# ---------------------------------------------------------------------------
def _flatten(bindings: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    for b in bindings:
        row: dict[str, str] = {}
        for key, val in b.items():
            raw = val["value"]
            if raw.startswith("http://www.wikidata.org/entity/"):
                raw = raw.split("/")[-1]
            row[key] = raw
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# VALUES-блок из списка QID
# ---------------------------------------------------------------------------
def _values_block(qids: list[str]) -> str:
    return " ".join(f"wd:{qid}" for qid in qids)


# ---------------------------------------------------------------------------
# Выгрузка одного типа связей для всего списка ID (батчами)
# ---------------------------------------------------------------------------
def fetch_relation(
    relation: str,
    query_template: str,
    all_ids: list[str],
) -> pd.DataFrame:
    sparql = _make_sparql()
    all_rows: list[dict[str, str]] = []
    total_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    log.info("▶ %s | батчей: %d × %d ID", relation, total_batches, BATCH_SIZE)

    for batch_idx in range(total_batches):
        batch = all_ids[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        query = query_template.format(values_block=_values_block(batch))

        try:
            bindings = _execute(sparql, query)
            rows = _flatten(bindings)
            all_rows.extend(rows)
        except Exception as exc:
            log.error(
                "  ✗ %s | батч %d/%d | ошибка: %s",
                relation, batch_idx + 1, total_batches, exc,
            )
            # Продолжаем — не падаем из-за одного батча
            time.sleep(REQUEST_DELAY * 3)
            continue

        if (batch_idx + 1) % 20 == 0 or batch_idx + 1 == total_batches:
            log.info(
                "  %s | %d/%d батчей | строк: %d",
                relation, batch_idx + 1, total_batches, len(all_rows),
            )

        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    log.info("✔ %s: итого %d строк", relation, len(df))
    return df


# ---------------------------------------------------------------------------
# Сохранение
# ---------------------------------------------------------------------------
def save_parquet(df: pd.DataFrame, name: str) -> Path:
    path = OUTPUT_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, compression="zstd")
    log.info("  → %s (%.1f MB)", path, path.stat().st_size / 1e6)
    return path


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def run(relation_names: list[str] | None = None) -> None:
    """
    Выгрузить связи для топ-сериалов.
    relation_names=None → все типы из RELATION_QUERIES.
    """
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"{INPUT_PATH} не найден. Сначала запусти шаги wikidata + pageviews."
        )

    log.info("Загружаем топ сериалов из %s...", INPUT_PATH)
    top_df = pd.read_parquet(INPUT_PATH)
    all_ids = top_df["wikidata_id"].tolist()
    log.info("Сериалов в списке: %d", len(all_ids))

    targets = relation_names or list(RELATION_QUERIES.keys())

    for relation in targets:
        if relation not in RELATION_QUERIES:
            log.warning("Неизвестный тип связи: '%s', пропускаем", relation)
            continue

        df = fetch_relation(relation, RELATION_QUERIES[relation], all_ids)
        if not df.empty:
            save_parquet(df, relation)
        else:
            log.warning("⚠ Пустой результат для '%s'", relation)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Filtered relations fetcher")
    parser.add_argument(
        "--relations",
        nargs="*",
        choices=list(RELATION_QUERIES.keys()),
        help="Какие связи выгружать (по умолчанию — все)",
    )
    args = parser.parse_args()
    run(args.relations)