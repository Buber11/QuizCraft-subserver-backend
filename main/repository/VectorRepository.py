import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Filter, FieldCondition, MatchValue


class VectorRepository:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VectorRepository, cls).__new__(cls)
            cls._instance.qdrant_client = QdrantClient(host="localhost", timeout=60, port=6333);
            cls._instance.collection_name = "sentence_vectors";
            cls._instance._create_collection_if_not_exists();
        return cls._instance


    def _create_collection_if_not_exists(self):
        try:
            self.qdrant_client.get_collection(self.collection_name)
        except Exception:
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=384,
                    distance=models.Distance.COSINE
                )
            )

    def save_vectors(self, points):
        response = self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=points
        )
        return response;


    def search_vectors(self, vector, user_id, limit=15):
        if isinstance(vector, np.ndarray):
            vector = vector.tolist()
        elif not isinstance(vector, list) or not all(isinstance(x, (float, int)) for x in vector):
            raise ValueError("query_vector must be a list of floats")

        response = self.qdrant_client.search(
            collection_name=self.collection_name,
            query_vector=vector,
            limit=limit,
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="user_id",
                        match=MatchValue(value=user_id))
                ]
            ),
            with_payload=True,
            with_vectors=True
        )
        return response