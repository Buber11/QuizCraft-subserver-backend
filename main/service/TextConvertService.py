import nltk
from sentence_transformers import SentenceTransformer
from main.repository.VectorRepository import VectorRepository


nltk.download('punkt_tab')

class TextConvertService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TextConvertService, cls).__new__(cls)
            cls._instance.vector_repository = VectorRepository()
            cls._instance.model = SentenceTransformer('all-MiniLM-L6-v2')
        return cls._instance

    def convertToVector(self, input_parameters: dict) -> None:
        print(input_parameters["text"])
        print(input_parameters["user_id"])
        sentences = nltk.sent_tokenize(input_parameters["text"])
        sentence_vectors = self.model.encode(sentences)
        for i, sentence in enumerate(sentences):
            print(f"Zdanie {i + 1}: {sentence}")
            print(f"Wektor {i + 1}: {len(sentence_vectors[i])}...")

        points = []
        for idx, (sentence, vector) in enumerate(zip(sentences, sentence_vectors)):
            point = {
                "id": idx,
                "vector": vector.tolist(),
                "payload": {
                    "sentence": sentence,
                    "user_id": input_parameters["user_id"],
                }
            }
            points.append(point)
            print(f"Point {idx}:")
            print(f"  ID: {point['id']}")
            print(f"  Sentence: {point['payload']['sentence']}")
            print(f"  Vector: {point['vector'][:5]}...")

        self.vector_repository.save_vectors(points)

    def get_vector(self, prompt, input_parameters) -> dict:
        search_sentences = nltk.sent_tokenize(prompt)
        search_vectors = self.model.encode(search_sentences)

        json_response = {
            "question": prompt,
            "sentences": []
        }

        for i, (sentence, vector) in enumerate(zip(search_sentences, search_vectors)):
            results = self.vector_repository.search_vectors(vector, "user_123", 5)

            print(f"Wyniki dla zdania {i + 1}:")
            for hit in results:
                json_response["sentences"].append({
                    "sentence": hit.payload['sentence'],
                    "score": float(hit.score)
                })

                print(f"  Podobieństwo: {hit.score:.4f}")
                print(f"  Zdanie: {hit.payload['sentence']}")

        print(json_response)
        return json_response


