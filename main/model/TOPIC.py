from enum import Enum


class TOPIC(Enum):
    AI_REQUEST_FOR_VECTOR_DATA = "ai-request-for-vector-data"
    AI_RESPONSE_FOR_VECTOR_DATA = "ai-response-for-vector-data"
    STATUS_PROCESSING = "status-processing"
    TEXT_PROCESSING_RQUEST = "text-processing-request"

    def get_topic_name(self):
        return self.value