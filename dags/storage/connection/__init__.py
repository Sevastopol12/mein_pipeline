import fsspec
from pathlib import PurePosixPath
from dataclasses import dataclass, field
import logging
from typing import Any

from ..credential import Credential, S3Credential
from ..utils import hash_file_content


logger = logging.getLogger(__name__)


@dataclass
class CloudStorageConnection:
    credential: Credential
    connection: fsspec.AbstractFileSystem
    storage_path: str
    session: Any = None
    supported_format: set[str] = field(
        default_factory=lambda: {".pdf", ".docx", "pptx", ".txt"}
    )

    @classmethod
    def establish_connection(cls, credential: Credential):
        protocol = credential.protocol
        bucket_name = credential.bucket_name

        storage_path = f"{protocol}://{bucket_name}"
        storage_options = credential._get_credentials()

        connection = fsspec.filesystem(
            protocol=protocol,
            **storage_options,
            asynchronous=True,
        )

        return cls(
            credential=credential, connection=connection, storage_path=storage_path
        )

    async def __aenter__(self):
        logger.info("Opening session...")
        self.session = await self.connection.set_session()

        logger.info(self.session)
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.critical(f"[{exc_type}]-[{exc_val}]-[{exc_tb}]")

        logger.info("Closing session...")

        await self.session.close()
        self.session = None
        logger.info(self.session)

    async def get_all_file_urls(self) -> list[str]:
        logger.info("Fetching files...")

        all_file_urls: list[str] = await self.connection._find(self.storage_path)
        valid_urls = self._filter_unsupported_format(all_file_urls)

        logger.info("All file urls fetched.")
        logger.info(f"{[valid_urls]}")

        return valid_urls

    async def hash_content(self, urls: list[str]) -> dict[str, str]:
        url_and_hash: set = {}
        logger.info("Hashing files...")

        for file_url in urls:
            file_content = await self.connection._cat_file(file_url)
            url_and_hash[file_url] = hash_file_content(file_content)

        logger.info("All file hashed...")

        return url_and_hash

    def _filter_unsupported_format(self, file_paths: list[str]) -> list[str]:
        valid_paths = [
            path
            for path in file_paths
            if PurePosixPath(path).suffix.lower() in self.supported_format
        ]

        return valid_paths


__all__ = ["CloudStorageConnection"]
