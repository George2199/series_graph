"""
wiki_texts.py
~~~~~~~~~~~~~
Выгружает текстовые описания (первый extract) для топ-N сериалов
через Wikipedia REST API. Async версия — параллельные запросы.

Вход:  data/processed/top_series_ids.parquet
Выход: data/processed/series_texts.parquet  (wikidata_id, title, summary)
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import aiohttp
import pandas as pd

log = logging.getLogger("wiki_texts")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------
WIKI_API = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
MAX_SUMMARY_CHARS = 2000
CONCURRENCY  = 100
TIMEOUT      = 20
MAX_RETRIES  = 3
RETRY_DELAY  = 2.0

INPUT_PATH  = Path("data/processed/top_series_ids.parquet")
OUTPUT_DIR  = Path("data/processed")
OUTPUT_PATH = OUTPUT_DIR / "series_texts.parquet"

HEADERS = {
    "User-Agent": "TVSeriesKnowledgeGraph/1.0",
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# Один async-запрос
# ---------------------------------------------------------------------------
async def fetch_summary(
    session: aiohttp.ClientSession,
    wikidata_id: str,
    title: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    url = WIKI_API.format(title=title)

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
                    if resp.status == 404:
                        return {"wikidata_id": wikidata_id, "title": title.replace("_", " "), "summary": None}
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    text = data.get("extract") or data.get("description") or ""
                    return {
                        "wikidata_id": wikidata_id,
                        "title": title.replace("_", " "),
                        "summary": text[:MAX_SUMMARY_CHARS] if text else None,
                    }
            except Exception as exc:
                log.debug("Ошибка '%s': %s, попытка %d", title, exc, attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)

    return {"wikidata_id": wikidata_id, "title": title.replace("_", " "), "summary": None}


# ---------------------------------------------------------------------------
# Параллельная выгрузка
# ---------------------------------------------------------------------------
async def fetch_all(rows: list[tuple[str, str]]) -> list[dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    total = len(rows)
    results: list[dict] = [{}] * total
    completed = 0
    t0 = time.time()

    async def tracked(i: int, wikidata_id: str, title: str) -> None:
        nonlocal completed
        results[i] = await fetch_summary(session, wikidata_id, title, semaphore)
        completed += 1
        if completed % 1000 == 0 or completed == total:
            elapsed = time.time() - t0
            rps = completed / elapsed
            eta = (total - completed) / rps if rps > 0 else 0
            log.info(
                "  Прогресс: %d / %d  |  %.1f req/s  |  ETA: %.0f мин",
                completed, total, rps, eta / 60,
            )

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 10)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        await asyncio.gather(*[tracked(i, wid, title) for i, (wid, title) in enumerate(rows)])

    return results


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def run() -> None:
    log.info("Загружаем топ сериалов...")
    df = pd.read_parquet(INPUT_PATH)
    total = len(df)
    log.info("Серий для обработки: %d  |  параллельность: %d", total, CONCURRENCY)

    rows = list(zip(df["wikidata_id"], df["title"]))

    t0 = time.time()
    records = asyncio.run(fetch_all(rows))
    elapsed = time.time() - t0
    log.info("Выгрузка завершена за %.1f сек (%.1f req/s)", elapsed, total / elapsed)

    result_df = pd.DataFrame(records)
    missing = result_df["summary"].isna().sum()
    log.info("Без текста: %d (%.1f%%)", missing, missing / total * 100)

    result_df = result_df.dropna(subset=["summary"])
    result_df.to_parquet(OUTPUT_PATH, index=False, compression="zstd")
    log.info("✔ Сохранено: %s (%d строк)", OUTPUT_PATH, len(result_df))


if __name__ == "__main__":
    run()