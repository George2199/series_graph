"""
pipeline.py — TV Series ETL Pipeline

Шаги:
  1. wikidata   — базовые свойства + wiki_links  → data/raw/
  2. pageviews  — фильтр топ-50k                 → data/processed/top_series_ids.parquet
  3. relations  — связи только для топ-50k        → data/raw/{relation}.parquet
  4. wiki_texts — тексты описаний                → data/processed/series_texts.parquet

Запуск:
  python pipeline.py                                        # все шаги
  python pipeline.py --steps wikidata pageviews             # первые два
  python pipeline.py --steps relations --relations cast genres
  python pipeline.py --resume                               # пропустить готовые
"""
from __future__ import annotations
import argparse, logging, sys, time
from pathlib import Path

log = logging.getLogger("pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

STEP_OUTPUTS: dict[str, list[Path]] = {
    "wikidata":   [Path("data/raw/series.parquet"), Path("data/raw/wiki_links.parquet")],
    "pageviews":  [Path("data/processed/top_series_ids.parquet")],
    "relations":  [Path("data/raw/cast.parquet")],
    "wiki_texts": [Path("data/processed/series_texts.parquet")],
}
ALL_STEPS = list(STEP_OUTPUTS.keys())
ALL_RELATIONS = ["cast", "creators", "directors", "genres", "countries", "languages", "based_on"]

def step_wikidata(query_names: list[str] | None = None) -> None:
    from wikidata_fetcher import run as wikidata_run
    queries = query_names or ["series", "wiki_links"]
    log.info("═══ ШАГ 1: Wikidata SPARQL (запросы: %s) ═══", queries)
    wikidata_run(queries)

def step_pageviews() -> None:
    from pageviews_filter import run as pv_run
    log.info("═══ ШАГ 2: Pageviews filter → топ-50k ═══")
    pv_run()

def step_relations(relation_names: list[str] | None = None) -> None:
    from relations_fetcher import run as rel_run
    relations = relation_names or ALL_RELATIONS
    log.info("═══ ШАГ 3: Relations для топ-50k (связи: %s) ═══", relations)
    rel_run(relations)

def step_wiki_texts() -> None:
    from wiki_texts import run as wt_run
    log.info("═══ ШАГ 4: Wikipedia texts ═══")
    wt_run()

def _is_done(step: str) -> bool:
    return all(p.exists() and p.stat().st_size > 0 for p in STEP_OUTPUTS.get(step, []))

def run(steps: list[str], resume: bool = False,
        query_names: list[str] | None = None,
        relation_names: list[str] | None = None) -> None:
    t0 = time.time()
    errors: list[str] = []
    for step in steps:
        if resume and _is_done(step):
            log.info("⏭  Пропускаем '%s' (output уже есть)", step)
            continue
        t_step = time.time()
        try:
            if step == "wikidata":      step_wikidata(query_names)
            elif step == "pageviews":   step_pageviews()
            elif step == "relations":   step_relations(relation_names)
            elif step == "wiki_texts":  step_wiki_texts()
            log.info("✔  '%s' завершён за %.1f сек", step, time.time() - t_step)
        except Exception as exc:
            log.error("✗  Шаг '%s' упал: %s", step, exc)
            errors.append(step)
    log.info("Пайплайн завершён за %.1f сек. Ошибки: %s", time.time() - t0, errors or "нет")
    if errors:
        sys.exit(1)

def main() -> None:
    from relations_fetcher import RELATION_QUERIES
    from queries import ALL_QUERIES
    parser = argparse.ArgumentParser(description="TV Series ETL Pipeline")
    parser.add_argument("--steps", nargs="+", choices=ALL_STEPS, default=ALL_STEPS)
    parser.add_argument("--queries", nargs="+", choices=list(ALL_QUERIES.keys()), default=None)
    parser.add_argument("--relations", nargs="+", choices=list(RELATION_QUERIES.keys()), default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    run(steps=args.steps, resume=args.resume, query_names=args.queries, relation_names=args.relations)

if __name__ == "__main__":
    main()