from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, TypeVar, List, Generic
import uuid
from dataclasses import dataclass, field


T = TypeVar('T')

class TaskStatus(Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def __str__(self):
        return self.value


class MethodProcessingType:
    QUIZ_PROCESSING = "QUIZ_PROCESSING"
    FLASHCARD_PROCESSING = "FLASHCARD_PROCESSING"
    FILL_IN_THE_BLANK_PROCESSING = "FILL_IN_THE_BLANK_PROCESSING"
    SUMMARY_PROCESSING = "SUMMARY_PROCESSING"
    TRUE_FALSE_PROCESSING = "TRUE_FALSE_PROCESSING"
    VECTOR_PROCESSING = "VECTOR_PROCESSING"

@dataclass
class ProcessingTask(Generic[T]):
    taskId: str = field(default_factory=lambda: str(uuid.uuid4()))
    methodProcessingType: str = None
    inputParameters: Dict[str, Any] = field(default_factory=dict)
    order: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[T] = None
    createdAt: datetime = None
    completedAt: Optional[datetime] = None
    expirationAt: Optional[datetime] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'ProcessingTask':
        inputParams = data.get("inputParameters", data.get("inputParameters", {}))

        task = ProcessingTask(
            taskId=data.get("taskId", str(uuid.uuid4())),
            methodProcessingType=data.get("methodProcessingType"),
            inputParameters=inputParams,
            order=data.get("order", 0),
            createdAt=data.get("createdAt"),
            status=TaskStatus.PENDING,
        )
        return task

    def to_dict(self) -> Dict[str, Any]:
        return {
            "taskId": self.taskId,
            "methodProcessingType": self.methodProcessingType,
            "inputParameters": self.inputParameters,
            "order": self.order,
            "status": str(self.status),
            "result": self.result,
            "createdAt": self.createdAt,
            "completedAt": self.completedAt,
            "expirationAt": self.expirationAt
        }

    def set_status(self, status: TaskStatus) -> None:
        self.status = status

    def set_result(self, result: T) -> None:
        self.result = result

    def set_completed_at(self, completed_at: datetime) -> None:
        self.completedAt = completed_at
        self.set_expiration_at(completed_at + timedelta(minutes=5))

    def set_expiration_at(self, expiration_at: datetime) -> None:
        self.expirationAt = expiration_at