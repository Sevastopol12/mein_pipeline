from supabase import create_async_client, AsyncClient
from .config import config
from contextlib import asynccontextmanager


@asynccontextmanager
async def get_async_client():
    if (config.connection_url.get("url") is None) and (
        config.connection_url.get("key") is None
    ):
        raise ValueError("No credentials.")

    client: AsyncClient = await create_async_client(
        supabase_url=config.connection_url.get("url"),
        supabase_key=config.connection_url.get("key"),
    )

    yield client
