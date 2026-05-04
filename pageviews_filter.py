"""
pageviews_filter.py — async aiohttp version (fixed for Python 3.12)
"""
from __future__ import annotations
import asyncio, logging, time
from pathlib import Path
from urllib.parse import quote
import aiohttp
import pandas as pd

log = logging.getLogger("pageviews_filter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

PAGEVIEWS_API = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/en.wikipedia/all-access/all-agents/{title}/monthly/{start}/{end}"
)
PERIOD_START = "20230101"
PERIOD_END   = "20231201"
TOP_N        = 40_000
CONCURRENCY  = 100
TIMEOUT      = 30
MAX_RETRIES  = 3
RETRY_DELAY  = 2.0

INPUT_PATH  = Path("data/raw/wiki_links.parquet")
OUTPUT_DIR  = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUTPUT_DIR / "top_series_ids.parquet"
HEADERS = {"User-Agent": "TVSeriesKnowledgeGraph/1.0"}


async def fetch_views(
    session: aiohttp.ClientSession,
    title: str,
    semaphore: asyncio.Semaphore,
) -> int:
    url = PAGEVIEWS_API.format(
        title=quote(title.replace(" ", "_"), safe=""),
        start=PERIOD_START,
        end=PERIOD_END,
    )
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
                    if resp.status == 404:
                        return 0
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 2))
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    return sum(item.get("views", 0) for item in data.get("items", []))
            except Exception as exc:
                log.debug("Ошибка '%s': %s, попытка %d", title, exc, attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY)
    return 0


async def fetch_all_views(titles: list[str]) -> list[int]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    total = len(titles)
    t0 = time.time()

    # Счётчик прогресса — обновляется по мере завершения задач
    completed_count = 0

    async def tracked(title: str) -> int:
        nonlocal completed_count
        result = await fetch_views(session, title, semaphore)
        completed_count += 1
        if completed_count % 500 == 0 or completed_count == total:
            elapsed = time.time() - t0
            rps = completed_count / elapsed
            eta = (total - completed_count) / rps if rps > 0 else 0
            log.info(
                "  Прогресс: %d / %d  |  %.1f req/s  |  ETA: %.0f мин",
                completed_count, total, rps, eta / 60,
            )
        return result

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 5)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        results = await asyncio.gather(*[tracked(title) for title in titles])

    return list(results)


def extract_title(article_url: str) -> str:
    return article_url.split("/wiki/")[-1]


def run() -> None:
    log.info("Загружаем wiki_links...")
    df = pd.read_parquet(INPUT_PATH)
    df["title"] = df["article"].apply(extract_title)
    df = df.drop_duplicates(subset="series")[["series", "title"]]
    total = len(df)
    log.info("Серий для обработки: %d  |  параллельность: %d", total, CONCURRENCY)

    t0 = time.time()
    views = asyncio.run(fetch_all_views(df["title"].tolist()))
    elapsed = time.time() - t0
    log.info("Выгрузка завершена за %.1f сек (%.1f req/s)", elapsed, total / elapsed)

    df["views_total"] = views
    top_df = (
        df.sort_values("views_total", ascending=False)
          .head(TOP_N)
          .rename(columns={"series": "wikidata_id"})
          [["wikidata_id", "title", "views_total"]]
          .reset_index(drop=True)
    )
    top_df.to_parquet(OUTPUT_PATH, index=False, compression="zstd")
    log.info(
        "✔ Топ-%d сохранён: %s  |  мин. просмотров: %d  |  макс: %d",
        TOP_N, OUTPUT_PATH,
        top_df["views_total"].min(),
        top_df["views_total"].max(),
    )


if __name__ == "__main__":
    run()