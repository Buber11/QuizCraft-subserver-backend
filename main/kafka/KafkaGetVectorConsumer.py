import json
import threading
import time
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv

from main.model.TOPIC import TOPIC
from main.model.Task import MethodProcessingType, TaskStatus, ProcessingTask
from main.service.TaskManagerSerive import TaskManagerService
from main.service.TextConvertService import TextConvertService


load_dotenv()
kafka_ip_host = os.getenv("kafka_ip_host")


class KafkaGetVectorConsumer:
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
                time.sleep(5)

    def _process_message(self, msg) -> None:
        try:
            json_data = json.loads(msg.value().decode('utf-8'))
            print(f'Received JSON message: {json_data}')

            task = ProcessingTask.from_dict(json_data)
            self.task_manager_service.add_task(task.taskId, task)

            self.task_manager_service.send_change_status(
                TOPIC.STATUS_PROCESSING,
                task.taskId,
                TaskStatus.PROCESSING
            )

            self._process_task(task)

        except json.JSONDecodeError:
            print(f"Invalid JSON format in message")
        except Exception as e:
            print(f"Error while processing message: {str(e)}")
            if 'task' in locals() and hasattr(task, 'taskId'):
                self.task_manager_service.send_change_status(
                    TOPIC.AI_RESPONSE_FOR_VECTOR_DATA,
                    task.taskId,
                    TaskStatus.FAILED
                )

    def _process_task(self, task: ProcessingTask) -> None:
        if self._is_vector_processing_task(task.methodProcessingType):
            self._process_vector_task(task)

    def _is_vector_processing_task(self, method_type: str) -> bool:
        vector_processing_types = [
            MethodProcessingType.QUIZ_PROCESSING,
            MethodProcessingType.FLASHCARD_PROCESSING,
            MethodProcessingType.FILL_IN_THE_BLANK_PROCESSING,
            MethodProcessingType.SUMMARY_PROCESSING,
            MethodProcessingType.TRUE_FALSE_PROCESSING,
            MethodProcessingType.VECTOR_PROCESSING
        ]
        return method_type in vector_processing_types

    def _process_vector_task(self, task: ProcessingTask) -> None:
        try:
            prompt = task.inputParameters.get("prompt")
            if not prompt:
                raise ValueError("Missing 'prompt' in task input parameters")

            vector = self.text_convert_service.get_vector(prompt, task.inputParameters)

            self.task_manager_service.send_result(
                TOPIC.AI_RESPONSE_FOR_VECTOR_DATA,
                task.taskId,
                vector
            )

        except Exception as e:
            print(f"Error processing vector task: {str(e)}")
            self.task_manager_service.send_change_status(
                TOPIC.AI_RESPONSE_FOR_VECTOR_DATA,
                task.taskId,
                TaskStatus.FAILED
            )

    def stop(self) -> None:
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        print("Vector consumer stopped")