import logging
import base64

from hashlib import sha256
from google.genai.types import File
from typing import Any


logger = logging.getLogger(__name__)


async def deduplicate(
    docs: list[str], client, return_uploaded: bool = True
) -> list[Any]:
    # Internal
    file_hash_mapping: dict[str, str] = {
        hash_file_content(doc_path): doc_path for doc_path in docs
    }

    # External
    get_stored_file_response = await client.files.list()
    stored_file = [file for file in get_stored_file_response]

    logger.info(f"In storage found: {[file.sha256_hash for file in stored_file]}")

    stored_file_mapping: dict[str, File] = {
        file.sha256_hash: file for file in stored_file
    }

    # Result
    files_to_upload: list[Any] = [
        file_path
        for hash_value, file_path in file_hash_mapping.items()
        if stored_file_mapping.get(hash_value, None) is None
    ]

    if return_uploaded:
        files_to_upload.extend([file for file in stored_file_mapping.values()])

    logger.info(f"Result: {files_to_upload}")

    return files_to_upload


def hash_file_content(file_path: str) -> str:
    hash_object = sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hash_object.update(chunk)

    digest = hash_object.hexdigest()

    del hash_object
    return base64.b64encode(digest.encode("utf-8")).decode("utf-8")
