import fsspec
import logging

from pathlib import PurePosixPath
from dataclasses import dataclass, field
from typing import Any, Optional
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    async_sessionmaker,
    AsyncSession,
)

from sqlalchemy import inspect, insert, update, select
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateSchema
from asyncpg.exceptions import UniqueViolationError

from dags.storage.models import Base, DataChunkStatus, StatusType
from ..credential import Credential, RDBCredential
from ..utils import hash_file_content, encode_file_content


logger = logging.getLogger(__name__)


@dataclass
class CloudStorageConnection:
    credential: Credential
    connection: fsspec.AbstractFileSystem
    storage_path: str
    session: Any = None
    supported_format: set[str] = field(
        default_factory=lambda: {".pdf", ".docx", ".pptx", ".txt"}
    )

    @classmethod
    def establish_connection(
        cls, credential: Credential, supported_format: Optional[set[str]] = None
    ):
        protocol = credential.protocol
        bucket_name = credential.bucket_name

        storage_path = f"{protocol}://{bucket_name}"
        storage_options = credential._get_credentials()

        connection = fsspec.filesystem(
            protocol=protocol,
            **storage_options,
            asynchronous=True,
        )
        kwargs = {
            "credential": credential,
            "connection": connection,
            "storage_path": storage_path,
        }

        if supported_format is not None:
            if not isinstance(supported_format, set):
                supported_format = set(supported_format)

            kwargs["supported_format"] = supported_format

        return cls(**kwargs)

    async def __aenter__(self):
        logger.info("Opening session...")
        self.session = await self.connection.set_session()

        logger.info(f"Session opened: [{self.session}]")
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.critical(f"[{exc_type}]-[{exc_val}]-[{exc_tb}]")

        logger.info(f"Closing session [{self.session}]...")

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

    async def hash_and_encode_content(self, urls: list[str]) -> dict[str, dict]:
        content_references: set = {}
        logger.info("Hashing files...")

        for file_url in urls:
            raw_bytes_content: bytes = await self.connection._cat_file(file_url)
            content_references[hash_file_content(raw_bytes_content)] = {
                "format": PurePosixPath(file_url).suffix.lower().replace(".", ""),
                "encoded_bytes": encode_file_content(raw_bytes_content),
            }

        logger.info("All file hashed...")

        return content_references

    def _filter_unsupported_format(self, file_paths: list[str]) -> list[str]:
        valid_paths = [
            path
            for path in file_paths
            if PurePosixPath(path).suffix.lower() in self.supported_format
        ]

        return valid_paths

    async def store_file(self, chunk_id: str, data_bytes: bytes):
        try:
            await self.connection._pipe_file(
                f"{self.storage_path}/chunk_{chunk_id}.parquet", data_bytes
            )

            return 1
        except Exception as e:
            logger.exception(e)
            return 0

    async def read_parquet_file(file_urls):
        pass


@dataclass
class CloudRDBConnection:
    engine: AsyncEngine
    AsyncSessionLocal: async_sessionmaker[AsyncSession]

    @classmethod
    def establish_connection(cls, credential: RDBCredential):
        engine: AsyncEngine = create_async_engine(url=credential._get_credentials())
        AsyncSessionLocal: AsyncSession = async_sessionmaker(
            bind=engine, autoflush=False, autocommit=False
        )

        return cls(
            engine=engine,
            AsyncSessionLocal=AsyncSessionLocal,
        )

    async def _create_table_and_schema(self, orm_object: Base):
        async with self.engine.begin() as connection:
            await connection.execute(
                CreateSchema(
                    orm_object.__table_args__.get("schema"), if_not_exists=True
                )
            )
            await connection.run_sync(orm_object.__table__.create, checkfirst=True)

    def _get_metadata(self, connection) -> dict[str, list[str]]:
        engine_inspector: Inspector = inspect(connection)
        ignores = [
            "auth",
            "extensions",
            "graphql",
            "graphql_public",
            "information_schema",
            "public",
            "realtime",
            "storage",
            "vault",
        ]
        all_schemas = engine_inspector.get_schema_names()

        metadata = {}
        for schema in all_schemas:
            if schema not in ignores:
                metadata[schema] = engine_inspector.get_table_names(schema=schema)

        return metadata

    def get_session(self):
        return self.AsyncSessionLocal.begin()

    async def table_and_schema(self):
        async with self.engine.connect() as connection:
            results = await connection.run_sync(self._get_metadata)
        return results


class DataChunkManager(CloudRDBConnection):
    table: Base = DataChunkStatus
    status_type = StatusType

    async def update_chunk_status(self, chunk_id: str, status: StatusType):
        async with self.get_session() as session:
            statement = (
                update(self.table)
                .where(self.table.chunk_id == chunk_id)
                .values(status=status)
            )

            result = await session.execute(statement)
        return result.rowcount

    async def record_chunk_status(
        self, chunk_id: str, status: StatusType = StatusType.PENDING
    ):
        async with self.get_session() as session:
            try:
                statement = insert(self.table).values(chunk_id=chunk_id, status=status)

                result = await session.execute(statement)
                return result.rowcount

            except UniqueViolationError as e:
                logger.exception(e)
                return 0

    async def all_chunk_status(self) -> list[dict]:
        async with self.get_session() as session:
            statement = select(self.table.__table__)

            results = await session.execute(statement)
            mapped_results: list[dict] = [dict(data) for data in results.mappings()]

            return mapped_results


__all__ = ["CloudStorageConnection", "CloudRDBConnection", "DataChunkManager"]
