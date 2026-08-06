import os
import logging
from dataclasses import dataclass, field
from dotenv import load_dotenv
from abc import abstractmethod


logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class Credential:
    @abstractmethod
    def _get_credentials(self) -> dict:
        """Return credentials for storage connection."""


@dataclass
class S3Credential(Credential):
    bucket_type: str
    bucket_name: str
    protocol: str = "s3"

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
    bucket_type: str
    bucket_name: str
    protocol: str = "gc"

    def _get_credentials(self) -> dict:
        pass


@dataclass
class RDBCredential(Credential):
    loc: str

    def _get_credentials(self) -> str:
        # Construct the SQLAlchemy connection string
        connection_string = os.getenv(f"{self.loc}_CONNECTION_STRING")
        return connection_string


if __name__ == "__main__":
    cred = S3Credential(bucket_type="s3", bucket_name="checkin_app")
    logger.info([cred._get_credentials()])


__all__ = ["S3Credential", "GCCredential", "RDBCredential"]
