from asyncio import create_task, gather
from .connection import get_async_client as get_supabase_client
from typing import Optional


async def fetch_all_docs(
    bucket_name: str, folder_name: Optional[str] = None
) -> list[bytes]:
    async with get_supabase_client() as client:
        if folder_name is not None:
            search_results: list[dict] = await client.storage.from_(bucket_name).list(
                folder_name
            )
            all_files = [f"{folder_name}/{file.name}" for file in search_results]
        else:
            search_results = await client.storage.from_("checkin_app").list_v2()
            all_files = [file.name for file in search_results.objects]

        file_references: list[bytes] = await gather(
            *[
                create_task(client.storage.from_("checkin_app").download(file))
                for file in all_files
            ]
        )

        return file_references


__all__ = ["fetch_all_docs", "get_supabase_client"]
