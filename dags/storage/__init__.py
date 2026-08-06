import logging
import pyarrow as pa
import pyarrow.parquet as pq

from pympler import asizeof

from asyncio import create_task, gather
from .models import Event, EventItem, Base, ParquetTable, StatusType
from .connection import (
    CloudStorageConnection,
    DataChunkManager,
    ApplicationDatabaseConnection,
    CloudRDBConnection,
)
from .credential import S3Credential, GCCredential, RDBCredential

logger = logging.getLogger(__name__)


async def fetch_from_storage(storage_provider: str, bucket_name: str):
    try:
        s3_credential = S3Credential(storage_provider, bucket_name)
        s3_connection = CloudStorageConnection.establish_connection(
            credential=s3_credential
        )

        async with s3_connection:
            file_urls = await s3_connection.get_all_file_urls()
            all_file_with_hash_value = await s3_connection.hash_and_encode_content(
                file_urls
            )

        return all_file_with_hash_value

    except Exception as e:
        logger.exception(e)
        return


async def load_to_temp_database_as_chunk(
    files: list[dict], max_mb_per_chunk: int = 20, max_file_per_chunk: int = 12
):
    staging_database_credential = S3Credential("STG", "staging")
    staging_database_connection: CloudStorageConnection = (
        CloudStorageConnection.establish_connection(
            credential=staging_database_credential
        )
    )

    chunk_manager_credential = RDBCredential()
    chunk_manager: DataChunkManager = DataChunkManager.establish_connection(
        credential=chunk_manager_credential
    )

    def convert_to_pybytes(data_chunk) -> bytes:
        buffer = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pydict(data_chunk), buffer)
        return buffer.getvalue().to_pybytes()

    async def keep_record(chunk_id: str, data_bytes: bytes) -> tuple[int, int]:
        tasks = [
            create_task(
                staging_database_connection.store_file(
                    chunk_id=chunk_id, data_bytes=data_bytes
                )
            ),
            create_task(
                chunk_manager.update_chunk_status(
                    chunk_id=chunk_id, status=StatusType.PENDING
                )
            ),
        ]

        data_store_result, status_result = await gather(*tasks)
        if status_result == 0:
            status_result = await chunk_manager.record_chunk_status(
                chunk_id=chunk_id, status=StatusType.PENDING
            )

        return (data_store_result, status_result)

    current_chunk = 1
    data_chunk: dict[str, list] = ParquetTable.get_template()
    current_chunk_size = asizeof.asizeof(data_chunk)
    current_chunk_length = 0

    async with staging_database_connection:
        for file in files:
            data_chunk["format"].append(file["format"])
            data_chunk["encoded_bytes"].append(file["encoded_bytes"])

            current_chunk_size += asizeof.asizeof(file)
            current_chunk_length += 1

            if (current_chunk_size >= max_mb_per_chunk * 1e6) or (
                current_chunk_length >= max_file_per_chunk
            ):
                results = await keep_record(
                    chunk_id=str(current_chunk),
                    data_bytes=convert_to_pybytes(data_chunk),
                )

                logger.info(f"chunk_{current_chunk}: {results}")

                # Reset
                data_chunk = ParquetTable.get_template()
                current_chunk_size = asizeof.asizeof(data_chunk)
                current_chunk_length = 0
                current_chunk += 1

        if current_chunk_length > 0:
            results = await keep_record(
                chunk_id=str(current_chunk),
                data_bytes=convert_to_pybytes(data_chunk),
            )
            logger.info(f"chunk_{current_chunk}: {results}")

    return


async def ingestion():
    storage_info = [("supabase", "checkin_app"), ("neon", "neoncheckin")]

    tasks = [
        create_task(
            fetch_from_storage(storage_provider=provider, bucket_name=bucket_name)
        )
        for provider, bucket_name in storage_info
    ]

    all_fetch_results = await gather(*tasks)

    deduplicated_files: dict[str, dict] = {}

    for result in all_fetch_results:
        if isinstance(result, dict):
            for hash_value, properties in result.items():
                deduplicated_files[hash_value] = properties

    all_files: list[dict] = [prop for prop in deduplicated_files.values()]

    await load_to_temp_database_as_chunk(
        files=all_files, max_mb_per_chunk=30, max_file_per_chunk=25
    )

    logger.info("Complete.")
    return


__all__ = [
    "Event",
    "EventItem",
    "Base",
    "CloudRDBConnection",
    "ApplicationDatabaseConnection",
    "CloudStorageConnection",
    "S3Credential",
    "GCCredential",
    "ingestion",
]
