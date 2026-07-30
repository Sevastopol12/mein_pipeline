import logging
from asyncio import Queue
from pydantic import BaseModel
from typing import Any

logger = logging.getLogger(__name__)


class PoisonPill:
    def __init__(self):
        """Kill worker loop"""
        pass


class ProductionQueue:
    def __init__(self, queue: Queue[dict | PoisonPill]):
        self.queue: Queue = queue
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
        logger.info(f"Got record: {record.get('object')} - {record.get('status')}")
        if record.get("status") == "SUCCEED":
            self.succeed_tasks.append(record.get("object"))
        else:
            self.failed_tasks.append(record.get("object"))

    def summarize(self) -> dict[str, list]:
        return {
            "succeed": self.succeed_tasks,
            "failed": self.failed_tasks,
        }


__all__ = ["ProductionQueue", "PoisonPill"]
