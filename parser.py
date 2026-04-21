import requests
import time
from tqdm import tqdm

from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

from qdrant import insert_batch

session = requests.Session()

headers = {
    "User-Agent": "WikiSeriesProject/1.0"
}

def normalize_title(title):
    return title.replace("_", " ")

def get_wiki_text(title, session):
    url = "https://en.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": True,
        "titles": normalize_title(title),
        "format": "json",
        "formatversion": 2,
        "redirects": 1
    }

    r = session.get(url, params=params, headers=headers, timeout=10)

    try:
        data = r.json()
    except Exception:
        print("NOT JSON RESPONSE:")
        print(r.text[:300])
        return ""

    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return ""

    return pages[0].get("extract", "")

def get_series_from_wikidata(session_headers, offset=0, limit=500):
    url = "https://query.wikidata.org/sparql"

    query = f"""
    SELECT ?item ?itemLabel ?article WHERE {{
      ?item wdt:P31 wd:Q5398426.
      ?article schema:about ?item;
               schema:isPartOf <https://en.wikipedia.org/>.

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
    text = get_wiki_text(item["title"], session)
    item["text"] = text
    return item

def pipeline(batch_limit=50, page_size=500):
    offset = 0

    with ThreadPoolExecutor(max_workers=10) as ex:

        futures = []

        for i in range(batch_limit):
            batch = get_series_from_wikidata(headers, offset, 500)

            if not batch:
                break

            offset += page_size

            time.sleep(2)

            for item in batch:
                futures.append(ex.submit(process, item))

        results = []
        for f in tqdm(as_completed(futures), total=len(futures)):
            results.append(f.result())

    return results

def safe_get_json(r):
    try:
        return r.json()
    except Exception:
        print("BAD RESPONSE:", r.text[:200])
        return None


results = pipeline(batch_limit=2)

print(results)

# insert_batch(results)