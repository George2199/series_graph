from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer
from qdrant_client.models import PointStruct

import hashlib

client = QdrantClient(host="localhost", port=6333)

def insert_series(item):
    vector = get_model().encode(item["text"]).tolist()

    point = PointStruct(
        id=item["id"],
        vector=vector,
        payload={
            "title": item["title"],
            "url": item["wiki_url"],
            "text": item["text"]
        }
    )

    client.upsert(
        collection_name="series",
        points=[point]
    )

def insert_batch(items):
    points = []

    for i, item in enumerate(items):
        if not item["text"]:
            continue

        vector = get_model().encode(item["text"]).tolist()

        points.append(
            PointStruct(
                id=make_id(item["wiki_url"]),
                vector=vector,
                payload={
                    "title": item["title"],
                    "url": item["wiki_url"],
                    "text": item["text"],
                }
            )
        )

    print(f"items={len(items)} → points={len(points)}")

    if not points:
        print("skip empty batch")
        return

    client.upsert(
        collection_name="series",
        points=points
    )

def search(query):
    vector = get_model().encode(query).tolist()

    return client.query_points(
        collection_name="series",
        query=vector,
        limit=10
    )

def make_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def init_qdrant(reset: bool = False):
    if reset:
        client.recreate_collection(
            collection_name="series",
            vectors_config=VectorParams(
                size=384,
                distance=Distance.COSINE
            )
        )
    else:
        # создаст, если нет
        if "series" not in [c.name for c in client.get_collections().collections]:
            client.create_collection(
                collection_name="series",
                vectors_config=VectorParams(
                    size=384,
                    distance=Distance.COSINE
                )
            )


def get_model():
    if not hasattr(get_model, "model"):
        get_model.model = SentenceTransformer("all-MiniLM-L6-v2")
    return get_model.model

init_qdrant()