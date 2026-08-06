import logging
import base64

from pydantic import BaseModel

from dags.tasks import extract, load_to_database

from dags.storage import (
    DataChunkManager,
    StatusType,
    RDBCredential,
    CloudStorageConnection,
    ApplicationDatabaseConnection,
    S3Credential,
)

logger = logging.getLogger(__name__)


def decode_and_convert_to_records(file_format: list[str], encoded_bytes: list[str]):
    storage = []
    for file_fmt, encoded_data in zip(file_format, encoded_bytes):
        if file_fmt and encoded_data:
            raw_bytes = base64.b64decode(encoded_data)
            storage.append({"format": file_fmt, "raw_bytes": raw_bytes})

    return storage


async def run_etl():
    models = ["gemma-4-31b-it", "gemma-4-26b-a4b-it"]

    data_chunk_manager = DataChunkManager.establish_connection(
        credential=RDBCredential(loc="STG")
    )
    staging_database_connection = CloudStorageConnection.establish_connection(
        credential=S3Credential(bucket_type="STG", bucket_name="staging"),
        supported_format=".parquet",
    )

    application_database_connection = ApplicationDatabaseConnection.establish_connection(
            credential=RDBCredential(loc="APL")
        )
    # Read DB
    all_chunk_status = await data_chunk_manager.all_chunk_status()

    async with staging_database_connection:
        for chunk_status in all_chunk_status:
            logger.info(f"Processing: {chunk_status}")

            if chunk_status["status"] != StatusType.PENDING:
                continue
            data_chunk = await staging_database_connection.read_parquet_file(
                chunk_id=chunk_status["chunk_id"]
            )

            tasks = decode_and_convert_to_records(
                file_format=data_chunk.get("format"),
                encoded_bytes=data_chunk.get("encoded_bytes"),
            )

            logger.info("Data converted to ideal format. Extracting content...")

            # Extraction
            results = await extract(tasks=tasks, models=models)

            batch_status: StatusType = results["status"]
            succeed_batch: list[BaseModel] = results["succeed"]

            logger.info("Storing results.")

            await load_to_database(
                database_connection=application_database_connection, data=succeed_batch
            )

            logger.info("Data loaded to Application DB.")

            rowcount = await data_chunk_manager.update_chunk_status(
                chunk_id=chunk_status["chunk_id"], status=batch_status
            )
            logger.info(f"Chunk marked as done: {'True' if rowcount else 'False'}")

    logger.info("Extract complete.")
    return 1
