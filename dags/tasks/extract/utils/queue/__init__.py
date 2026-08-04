import logging
from asyncio import Queue
from pydantic import BaseModel
from typing import Any

from dags.storage import StatusType


logger = logging.getLogger(__name__)


class PoisonPill:
    def __init__(self):
        """Kill worker loop"""
        pass


class ProductionQueue:
    def __init__(self, queue: Queue[dict | PoisonPill]):
        self.queue: Queue = queue
        self.status: StatusType = StatusType.DONE
        self.succeed_tasks: list[BaseModel] = []
        self.failed_tasks: list[dict] = []

    @classmethod
    def _create(cls, tasks: list[dict], n_workers: int = 1):
        production_queue = Queue()

        for task in tasks:
            if isinstance(task, dict):
                production_queue.put_nowait(task)

        for _ in range(n_workers):
            production_queue.put_nowait(PoisonPill())

        return cls(queue=production_queue)

    def record(self, record: dict[str, Any]):
        logger.info(f"Got record: {record.get('status')}")
        if record.get("status") == "SUCCEED":
            self.succeed_tasks.append(record.get("object"))
        else:
            self.failed_tasks.append(record.get("object"))

    def summarize(self) -> dict[str, list]:
        return {
            "status": self.status,
            "succeed": self.succeed_tasks,
            "failed": self.failed_tasks,
        }

    def assign_for_review(self):
        self.status = StatusType.ERROR
        logger.critical("Batch failed unexpectedly. Review required...")

    def assign_for_rerun(self):
        self.status = StatusType.PENDING
        logger.critical("Batch failed unexpectedly. Assigning for re-run...")


__all__ = ["ProductionQueue", "PoisonPill"]
