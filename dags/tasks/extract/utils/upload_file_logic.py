import logging
import base64

from asyncio import Semaphore, gather, create_task
from hashlib import sha256
from pathlib import Path
from google.genai.types import File
from typing import Any

logger = logging.getLogger(__name__)


async def upload_event_files(
    docs: list[Any], client, max_workers: int = 5
) -> list[File]:
    uploaded_files = [file for file in docs if isinstance(file, File)]
    semaphore = Semaphore(max_workers)

    tasks = [
        create_task(_upload(file_path=file_path, client=client, semaphore=semaphore))
        for file_path in docs
        if isinstance(file_path, str)
    ]

    upload_results = await gather(*tasks, return_exceptions=True)

    logger.info(f"Uploaded {len(upload_results)} files.")
    uploaded_files.extend(upload_results)

    del upload_results, tasks

    return uploaded_files


def hash_file_content(file: str | bytes) -> str:
    if isinstance(file, str):
        hash_object = sha256()

        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_object.update(chunk)
    else:
        hash_object = sha256(file)

    digest = hash_object.hexdigest()
    
    del hash_object
    return base64.b64encode(digest.encode("utf-8")).decode("utf-8")


async def _upload(file_path: str, client, semaphore: Semaphore) -> File:
    async with semaphore:
        try:
            path = Path(file_path)
            uploaded_file = await client.files.upload(file=path)

            return uploaded_file
        except Exception as e:
            logger.exception(e)
            return None
