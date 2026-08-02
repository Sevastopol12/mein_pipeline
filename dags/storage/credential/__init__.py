import os
import logging
from dataclasses import dataclass, field
from dotenv import load_dotenv
from abc import abstractmethod
from typing import Optional


logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class Credential:
    @abstractmethod
    def _get_credentials(self) -> dict:
        """Return credentials for storage connection."""


@dataclass
class S3Credential(Credential):
    protocol: str = "s3"
    bucket_type: str
    bucket_name: str

    def _get_credentials(self) -> dict:
        """Return credentials for storage connection."""
        storage_options = {
            "key": os.getenv(f"{self.bucket_type.upper()}_KEY"),
            "secret": os.getenv(f"{self.bucket_type.upper()}_SECRET"),
            "client_kwargs": {
                "endpoint_url": os.getenv(f"{self.bucket_type.upper()}_ENDPOINT_URL"),
                "region_name": os.getenv(f"{self.bucket_type.upper()}_REGION_NAME"),
            },
        }

        return storage_options


@dataclass
class GCCredential(Credential):
    protocol: str = "gc"
    bucket_type: str
    bucket_name: str

    def _get_credentials(self) -> dict:
        pass


@dataclass
class RDBCredential(Credential):
    def _get_credentials(self) -> str:
        # Construct the SQLAlchemy connection string
        connection_string = f"postgresql+asyncpg://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DBNAME')}?ssl=require"
        return connection_string


if __name__ == "__main__":
    cred = S3Credential(bucket_type="s3", bucket_name="checkin_app")
    logger.info([cred._get_credentials()])


__all__ = ["S3Credential", "GCCredential", "RDBCredential"]
