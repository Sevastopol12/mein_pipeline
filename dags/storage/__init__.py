import logging
import os
from asyncio import create_task, gather, Future
from .sources import fetch_supabase_s3
from .models import Event, EventItem, Base

logger = logging.getLogger(__name__)


async def fetch_all_sources():
    results: list[Future[list]] = await gather(
        *[create_task(fetch_supabase_s3(os.get("SUPABASE_BUCKET_NAME")))]
    )

    files = []
    for fetch_result in results:
        files.extend(fetch_result)


__all__ = [
    "Event",
    "EventItem",
    "Base",
]
