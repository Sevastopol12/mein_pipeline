from asyncio import create_task, gather
from .models import Event, EventItem, Base
from .connection import CloudStorageConnection
from .credential import S3Credential, GCCredential


async def fetch_from_storage(storage_provider: str, bucket_name: str):
    try:
        s3_credential = S3Credential(storage_provider, bucket_name)
        s3_connection = CloudStorageConnection.establish_connection(
            credential=s3_credential
        )

        async with s3_connection:
            file_urls = await s3_connection.get_all_file_urls()
            all_file_with_hash_value = await s3_connection.hash_content(file_urls)

        return all_file_with_hash_value

    except Exception as e:
        raise


async def ingestion() -> list[dict[str, str | bytes]]:
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

    return [prop for prop in deduplicated_files.values()]


__all__ = [
    "Event",
    "EventItem",
    "Base",
    "CloudStorageConnection",
    "S3Credential",
    "GCCredential",
    "ingestion",
]
