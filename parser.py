import requests
import time
from tqdm import tqdm
import re

from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

from qdrant import insert_batch, client, search

session = requests.Session()

headers = {
    "User-Agent": "WikiSeriesProject/1.0 (contact: 1georgedenisov0@gmail.com)"
}

def normalize_title(title):
    return title.replace("_", " ")

def get_wiki_text(title, session):
    api_url = "https://en.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": True,
        "titles": normalize_title(title),
        "format": "json",
        "formatversion": 2,
        "redirects": 1
    }

    r = session.get(api_url, params=params, headers=headers, timeout=10)

    try:
        data = r.json()
    except Exception:
        return ""

    pages = data.get("query", {}).get("pages", [])
    if not pages:
        print("NOT JSON RESPONSE:")
        print(r.text[:300])
        return ""

    return pages[0].get("extract", "")

def get_series_from_wikidata(offset=0, limit=500):
    url = "https://query.wikidata.org/sparql"

    query = f"""
    SELECT DISTINCT ?item ?itemLabel ?article WHERE {{
    VALUES ?type {{
        wd:Q5398426
        wd:Q21191270
    }}

    ?item wdt:P31 ?type.

    ?article schema:about ?item;
            schema:isPartOf <https://en.wikipedia.org/>;
            schema:inLanguage "en".

    SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en".
    }}
    }}
    LIMIT {limit}
    OFFSET {offset}
    """

    for i in range(3):
        r = requests.get(url, params={"query": query, "format": "json"}, headers=headers)
        data = safe_get_json(r)

        if data:
            break

        time.sleep(2)
        
    if not data:
        return None

    results = []
    for item in data["results"]["bindings"]:
        results.append({
            "title": item["itemLabel"]["value"],
            "wiki_url": item["article"]["value"]
        })

    return results

def process(item):
    time.sleep(0.2)
    text = get_wiki_text(item["title"], session)
    text = clean_wiki_text(text)

    # print("TEXT LEN:", len(text))

    if not text or len(text) < 300:
        return None
    
    item["text"] = text
    return item

def pipeline(page_size=500):
    offset = 0

    with ThreadPoolExecutor(max_workers=2) as ex:
        while True:
            batch = get_series_from_wikidata(offset, page_size)

            if not batch:
                print("DONE")
                break

            print(f"OFFSET {offset}, batch={len(batch)}")

            futures = [ex.submit(process, item) for item in batch]

            results = []
            for f in tqdm(as_completed(futures), total=len(futures)):
                res = f.result()
                if res:
                    results.append(res)

            insert_batch(results)  # ← сразу пишем

            offset += page_size
            time.sleep(2)

    return results

def safe_get_json(r):
    try:
        return r.json()
    except Exception:
        print("BAD RESPONSE:", r.text[:200])
        return None
    
def clean_wiki_text(text: str) -> str:
    if not text:
        return ""

    # Удаляем секции типа == References == и всё после них
    text = re.split(r"\n==\s*(References|External links|See also|Notes|Bibliography)\s*==", text)[0]

    # Удаляем все заголовки == Something ==
    text = re.sub(r"\n==.*?==\n", "\n", text)

    # Удаляем лишние переносы
    text = re.sub(r"\n{2,}", "\n", text)

    return text.strip()

results = pipeline()

for item in results[:20]:
    print(item["title"])
    print(len(item["text"]))
    print("----")

print(results[-1])

print("results count:", len(results))

insert_batch(results)
print(client.get_collections())

points, _ = client.scroll("series", limit=5)

print("INSERTING:", len(points))

for p in points:
    if not p.payload:
        continue
    print(p.id, p.payload["title"])

res = search("spy adventure")

for r in res.points:
    if not r.payload:
        continue
    print(r.payload["title"], r.score)