import datetime
import json
import os
import threading


from confluent_kafka import Consumer
from dotenv import load_dotenv

from main.model import TOPIC
from main.model.Task import ProcessingTask, TaskStatus
from main.service.TaskManagerSerive import TaskManagerService
from main.service.TextConvertService import TextConvertService

load_dotenv()
kafka_ip_host = os.getenv("kafka_ip_host")


class KafkaUploadFileConsumer:
    def __init__(self):
        self.text_convert_service = TextConvertService()
        self.task_manager_service = TaskManagerService()
        self.consumer = None
        self.running = False
        self.consumer_thread = None

    def start(self, topic: str, group_id: str) -> None:

        config = {
            'bootstrap.servers': kafka_ip_host,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }

        self.consumer = Consumer(config)
        self.consumer.subscribe([topic])

        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        print(f"Vector consumer started for topic: {topic}")

    def _consume(self) -> None:
        while self.running:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                self._process_message(msg)

            except Exception as e:
                print(f"Error in Kafka consumer: {str(e)}")

    def _process_message(self, msg) -> None:
        try:
            json_data = json.loads(msg.value().decode('utf-8'))
            task = ProcessingTask.from_dict(json_data)
            self.task_manager_service.add_task(task.taskId, task)

            self.task_manager_service.send_change_status(
                TOPIC.TOPIC.STATUS_PROCESSING,
                task.taskId,
                TaskStatus.PROCESSING
            )

            self.text_convert_service.convertToVector(task.inputParameters)

            self.task_manager_service.add_task(task.taskId, task)
            self.task_manager_service.send_change_status(
                TOPIC.TOPIC.STATUS_PROCESSING,
                task.taskId,
                TaskStatus.COMPLETED
            )
        except Exception as e:
            print(f"Error processing message: {str(e)}")