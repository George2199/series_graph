Qdrant start:

```
docker run -p 6333:6333 -p 6334:6334 \
  -v qdrant_storage:/qdrant/storage \
  qdrant/qdrant
```

Qdrant for dev:
```
docker run -p 6333:6333 -p 6334:6334 \
  -v $(pwd)/qdrant_data:/qdrant/storage \
  qdrant/qdrant
```

# 1. Базовые данные — быстро (~10-20 мин)
python pipeline.py --steps wikidata

# 2. Фильтр по просмотрам — долго (~2 часа, 50k HTTP-запросов)
python pipeline.py --steps pageviews

# 3. Связи только для топ-50k — долго (~3-5 часов, 100 батчей × 7 типов)
python pipeline.py --steps relations

# 4. Тексты описаний — долго (~2 часа)
python pipeline.py --steps wiki_texts

# Или всё сразу на ночь:
python pipeline.py