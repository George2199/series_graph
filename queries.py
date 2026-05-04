"""
SPARQL-запросы для выгрузки сериалов и их связей из Wikidata.
Каждый запрос возвращает один тип данных — пагинация через LIMIT/OFFSET.
"""

# ---------------------------------------------------------------------------
# Базовый запрос: сериалы с основными свойствами
# ---------------------------------------------------------------------------
SERIES_BASE = """
SELECT DISTINCT
  ?series ?seriesLabel
  ?startYear ?endYear
  ?episodeCount ?seasonCount
WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .   # instance of: television series

  OPTIONAL {{ ?series wdt:P580 ?start .
              BIND(YEAR(?start) AS ?startYear) }}
  OPTIONAL {{ ?series wdt:P582 ?end .
              BIND(YEAR(?end)   AS ?endYear)   }}
  OPTIONAL {{ ?series wdt:P1113 ?episodeCount }}
  OPTIONAL {{ ?series wdt:P4908 ?seasonCount  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Актёры
# ---------------------------------------------------------------------------
CAST = """
SELECT ?series ?person ?personLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P161 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Создатели / showrunner  (P162 — producer; P57 — director at series level;
# здесь берём P162 как "creator/executive producer")
# ---------------------------------------------------------------------------
CREATORS = """
SELECT ?series ?person ?personLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P162 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Режиссёры (P57 — director, на уровне сериала)
# ---------------------------------------------------------------------------
DIRECTORS = """
SELECT ?series ?person ?personLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P57 ?person .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Жанры
# ---------------------------------------------------------------------------
GENRES = """
SELECT ?series ?genre ?genreLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P136 ?genre .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Страны производства
# ---------------------------------------------------------------------------
COUNTRIES = """
SELECT ?series ?country ?countryLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P495 ?country .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Языки
# ---------------------------------------------------------------------------
LANGUAGES = """
SELECT ?series ?language ?languageLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P364 ?language .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Оригинальное произведение (адаптации)
# ---------------------------------------------------------------------------
BASED_ON = """
SELECT ?series ?work ?workLabel WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?series wdt:P144 ?work .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ru" . }}
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# ---------------------------------------------------------------------------
# Pageviews-фильтр: Wikipedia page_id → нужен для Pageviews API
# Вытаскиваем sitelink на английскую Wikipedia
# ---------------------------------------------------------------------------
WIKIPEDIA_LINKS = """
SELECT ?series ?article WHERE {{
  ?series wdt:P31/wdt:P279* wd:Q5398426 .
  ?article schema:about ?series ;
           schema:inLanguage "en" ;
           schema:isPartOf <https://en.wikipedia.org/> .
}}
ORDER BY ?series
LIMIT  {limit}
OFFSET {offset}
"""

# Словарь для итерации по всем типам запросов
ALL_QUERIES = {
    "series":     SERIES_BASE,
    "cast":       CAST,
    "creators":   CREATORS,
    "directors":  DIRECTORS,
    "genres":     GENRES,
    "countries":  COUNTRIES,
    "languages":  LANGUAGES,
    "based_on":   BASED_ON,
    "wiki_links": WIKIPEDIA_LINKS,
}