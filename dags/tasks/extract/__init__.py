import logging

from asyncio import create_task, gather
from pydantic import BaseModel

from .connection import get_async_client
from .utils import (
    ProductionQueue,
    ExtractorConfig,
    Extractor,
)
from dags.storage import EventItem


logger = logging.getLogger(__name__)


async def assign_worker(async_client, queue: ProductionQueue, config: ExtractorConfig):
    logger.info(f"Assigning: {config.model_name}")
    status = await Extractor(async_client, config=config).process(queue)
    return status


async def extract(tasks: list[dict], models: list[str]):
    try:
        queue = ProductionQueue._create(tasks=tasks, n_workers=len(models))
        instruction: str = """
        - Determine if the document describes an event hosted by a person or organization.
        - **If YES (`is_event = true`)**: Extract and populate all relevant fields based on the content.
        - **If NO (`is_event = false`)**: Set `is_event` to false, extract and populate 'title' field, and set ALL other string fields (`content`, `participants`, `person_in_charge`, `contact`, `location`, `notes`, `start_at`, `end_at`) to an empty string `""`.
        """
        response_schema: BaseModel = EventItem

        async with get_async_client() as async_client:
            tasks = [
                create_task(
                    assign_worker(
                        async_client=async_client,
                        queue=queue,
                        config=ExtractorConfig(
                            model_name=model,
                            instruction=instruction,
                            response_schema=response_schema,
                            rpm=15,
                            max_attempts=15,
                        ),
                    )
                )
                for model in models
            ]
            logger.info(f"Task assigned: [{tasks}]")
            logger.info("Waiting...")

            await gather(*tasks)

        logger.info("All task done. Stopping workers...")

        results = queue.summarize()
        return results

    except Exception as e:
        logger.exception(e)
        return 0


__all__ = ["extract"]
