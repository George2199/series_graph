from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer
from qdrant_client.models import PointStruct

client = QdrantClient(host="localhost", port=6333)

client.recreate_collection(
    collection_name="series",
    vectors_config=VectorParams(
        size=384,  # для all-MiniLM-L6-v2
        distance=Distance.COSINE
    )
)

model = SentenceTransformer("all-MiniLM-L6-v2")

def insert_series(item):
    vector = model.encode(item["text"]).tolist()

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

        vector = model.encode(item["text"]).tolist()

        points.append(
            PointStruct(
                id=item["wiki_url"],
                vector=vector,
                payload={
                    "title": item["title"],
                    "url": item["wiki_url"],
                    "text": item["text"],
                }
            )
        )

    client.upsert(
        collection_name="series",
        points=points
    )

def search(query):
    vector = model.encode(query).tolist()

    return client.query_points(
        collection_name="series",
        query=vector,
        limit=10
    )