from qdrant_client import QdrantClient
from qdrant_client.models import *

class QdrantStorage:
    def __init__(self, url: str = "http://localhost:6333", collection: str = "docs", dim: int = 3072):
        self.client = QdrantClient(url=url,timeout=30)
        self.collection = collection
        if not self.client.collection_exists(self.collection):
            self.client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(size=dim,distance=Distance.COSINE)
            )

    #handles the adding of vectors
    def upsert(self, ids, vectors, payloads):
        points = [PointStruct(id=ids[i],vector=vectors[i],payload=payloads[i]) for i in range(len(ids))]
        self.client.upsert(self.collection, points)

    #handles the searching of vectors
    def search(self, query_vector, top_k: int = 5):
        results = self.client.search(
            collection_name=self.collection,
            query_vector=query_vector,
            with_payload=True,
            limit=top_k
        )
        contexts = []
        sources = set()

        for r in results:
            paylaod = getattr(r, 'payload', None) or {}
            text = paylaod.get('text',"")
            source = paylaod.get('source',"")

            if text:
                contexts.append(text)
                sources.add(source)
    #top_k = 5 means that it will search for the top 5 results from the DB

        return {"contexts": contexts, "sources": sources}


