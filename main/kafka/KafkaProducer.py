from confluent_kafka import Producer
import os


class KafkaProducer:
    def __init__(self):
        kafka_ip_host = os.getenv("KAFKA_IP_HOST")
        self.producer = Producer({'bootstrap.servers': kafka_ip_host})

    def send(self, topic, json_task):
        try:
            self.producer.produce(topic.get_topic_name(), json_task)
            self.producer.flush()
            print(f"Sending message to {topic}: {json_task}")
        except Exception as e:
            print(f"Error while sending message to Kafka: {str(e)}")
