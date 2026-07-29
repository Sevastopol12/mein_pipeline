import logging
from google.genai.types import File
from asyncio import Queue
from typing import Any

logger = logging.getLogger(__name__)


class PoisonPill:
    def __init__(self):
        """Kill worker loop"""
        pass


class ProductionQueue:
    def __init__(self, queue: Queue[File | PoisonPill]):
        self.production_queue: Queue = queue
        self.succeed_tasks: list = []
        self.failed_tasks: list = []

    @classmethod
    def _create(cls, tasks: list[File], n_workers: int = 1):
        production_queue = Queue()

        for task in tasks:
            if isinstance(task, File):
                production_queue.put_nowait(task)

        for _ in n_workers:
            production_queue.put_nowait(PoisonPill())

        return cls(queue=production_queue)

    def record(self, record: dict[str, Any]):
        logger.info(f"Got record: {record.get('object')} - {record.get('status')}")
        if record.get("status") == "SUCCEED":
            self.succeed_tasks.append(record.get("object"))
        else:
            self.failed.append(record.get("object"))

    def summarize(self) -> dict[str, list]:
        return {
            "succeed": self.succeed_tasks,
            "failed": self.failed_tasks,
        }


__all__ = ["ProductionQueue", "PoisonPill"]
