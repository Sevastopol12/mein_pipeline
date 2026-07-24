import logging

from .connection import get_async_client
from .utils import (
    get_documents,
    deduplicate,
    upload_event_files,
    extract_file_contents,
    load_to_storage,
    PoisonPill as stop_condition,
)

from asyncio import Queue, gather, create_task

from airflow.sdk import task
from dags.storage import EventItem as RESPONSE_SCHEMA


logger = logging.getLogger(__name__)


@task(task_id="extract")
async def run_extract():
    models = ["gemma-4-31b-it", "gemma-4-26b-a4b-it"]
    documents = get_documents()

    async with get_async_client() as client:
        file_to_upload = await deduplicate(documents, client)
        upload_results = await upload_event_files(file_to_upload, client)

        task_queue = Queue()
        # Assign task
        for file in upload_results:
            task_queue.put_nowait(file)

        # Stop condition
        for _ in range(len(models)):
            task_queue.put_nowait(stop_condition())

        instruction = "Extract content from the following file: "
        tasks = [
            create_task(
                extract_file_contents(
                    pending_queue=task_queue,
                    client=client,
                    model_name=model_name,
                    instruction=instruction,
                    response_schema=RESPONSE_SCHEMA,
                )
            )
            for model_name in models
        ]

        results = await gather(*tasks)

        logger.info("All task completed. Shutting down workers...")
        for task in tasks:
            task.cancel()
        logger.info("All workers shutdown.")

        load_to_storage(results)


__all__ = ["get_async_client", "run_extract"]
