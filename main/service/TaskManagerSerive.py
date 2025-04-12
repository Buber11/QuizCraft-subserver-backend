import json
import uuid

from main.kafka.KafkaProducer import KafkaProducer
from main.model.Task import ProcessingTask
from main.struct.ThreadSafeDict import ThreadSafeDict


class TaskManagerService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TaskManagerService, cls).__new__(cls)
            cls._instance.tasks = ThreadSafeDict()
            cls._instance.producer = KafkaProducer()
        return cls._instance

    def add_task(self, task_id, task):
        self.tasks.put(task_id, task)

    def create_task(self, TOPIC, method_processing_type, input_parameters):
        task_id = str(uuid.uuid4())
        task = ProcessingTask(
            task_id=task_id,
            method_processing_type=method_processing_type,
            input_parameters=input_parameters,
            order=1,
        )
        task_json = json.dumps(task.__dict__, default=str)
        self.producer.send(TOPIC, task_json);

    def send_result(self,TOPIC, task_id, result):
        task = self.tasks.get(task_id)
        if task:
            task.set_result(result)
        task_json = json.dumps(task.to_dict(), default=str)
        self.producer.send(TOPIC, task_json)

    def send_change_status(self, TOPIC, task_id, status):
        task = self.tasks.get(task_id)
        if task:
            task.set_status(status)
            self.tasks.put(task_id, task)
        task_json = json.dumps(task.to_dict(), default=str)
        print("wysłano ", task_json)
        self.producer.send(TOPIC, task_json)

