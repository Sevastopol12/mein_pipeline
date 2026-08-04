import logging
import json
import base64

from asyncio import create_task, gather
from pydantic import BaseModel

from .connection import get_async_client
from .utils import (
    ProductionQueue,
    ExtractorConfig,
    Extractor,
    decode_and_convert_to_records,
)
from dags.storage import (
    EventItem,
    DataChunkManager,
    StatusType,
    RDBCredential,
    CloudStorageConnection,
    S3Credential,
)


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


async def run_extraction():
    models = ["gemma-4-31b-it", "gemma-4-26b-a4b-it"]

    data_chunk_manager = DataChunkManager.establish_connection(
        credential=RDBCredential()
    )
    staging_database_connection = CloudStorageConnection.establish_connection(
        credential=S3Credential(bucket_type="STG", bucket_name="staging"),
        supported_format=".parquet",
    )

    all_chunk_status = await data_chunk_manager.all_chunk_status()

    async with staging_database_connection:
        for chunk_status in all_chunk_status:
            logger.info(f"Processing: {chunk_status}")
            if chunk_status["status"] == StatusType.PENDING:
                data_chunk = await staging_database_connection.read_parquet_file(
                    chunk_id=chunk_status["chunk_id"]
                )

            tasks = decode_and_convert_to_records(
                file_format=data_chunk.get("format"),
                encoded_bytes=data_chunk.get("encoded_bytes"),
            )

            logger.info("Data converted to ideal format. Extracting content...")

            results = await extract(tasks=tasks, models=models)

            succeed: list[BaseModel] = results["succeed"]
            failed: list[dict] = results["failed"]

            logger.info("Storing results.")

            with open("tester/succeed.jsonl", "w", encoding="utf-8") as file:
                data = "\n".join(
                    item.model_dump_json() for item in succeed if item.is_event
                )
                file.write(data)

            with open("tester/failed.jsonl", "w", encoding="utf-8") as file:
                for obj in failed:
                    if isinstance(obj['raw_bytes'], bytes):
                        obj['raw_bytes'] = base64.b64encode(obj['raw_bytes']).decode('utf-8')
                    file.write(json.dumps(obj) + "\n")

            logger.info("Done.")


            if results:
                rowcount = await data_chunk_manager.update_chunk_status(
                    chunk_id=chunk_status["chunk_id"], status=StatusType.DONE
                )
                logger.info(f"Chunk marked as done: {'True' if rowcount else 'False'}")

    logger.info("Extract complete.")
    return 1


__all__ = ["run_extraction", "extract"]
