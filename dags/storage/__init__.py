import logging
import pyarrow as pa
import pyarrow.parquet as pq

from pympler import asizeof

from asyncio import create_task, gather
from .models import Event, EventItem, Base, ParquetTable
from .connection import CloudStorageConnection
from .credential import S3Credential, GCCredential

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
    def convert_to_pybytes(data_chunk) -> bytes:
        buffer = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pydict(data_chunk), buffer)
        return buffer.getvalueof().to_pybytes()

    staging_database_credential = S3Credential("STG", "staging")
    staging_database_connection = CloudStorageConnection.establish_connection(
        credential=staging_database_credential
    )

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
                await staging_database_connection.dump_parquet_file_to_bucket(
                    chunk_name=f"chunk_{current_chunk}.parquet",
                    data_bytes=convert_to_pybytes(data_chunk),
                )

                # Reset
                data_chunk = ParquetTable.get_template()
                current_chunk_size = asizeof.asizeof(data_chunk)
                current_chunk_length = 0
                current_chunk += 1

        if current_chunk_length > 0:
            await staging_database_connection.dump_parquet_file_to_bucket(
                chunk_name=f"chunk_{current_chunk}.parquet",
                data_bytes=convert_to_pybytes(data_chunk),
            )
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
        files=all_files, max_mb_per_chunk=20, max_file_per_chunk=12
    )

    logger.info("Complete.")
    return


__all__ = [
    "Event",
    "EventItem",
    "Base",
    "CloudStorageConnection",
    "S3Credential",
    "GCCredential",
    "ingestion",
]
