import os
import logging
from dataclasses import dataclass
from dotenv import load_dotenv
from abc import abstractmethod

logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class Credential:
    bucket_type: str
    bucket_name: str

    @abstractmethod
    def _get_credentials(self) -> dict:
        """Return credentials for storage connection."""


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


class GCCredential(Credential):
    protocol: str = "gc"
    bucket_type: str
    bucket_name: str

    def _get_credentials(self) -> dict:
        pass


if __name__ == "__main__":
    cred = S3Credential(bucket_type="s3", bucket_name="checkin_app")
    logger.info([cred._get_credentials()])


__all__ = ["S3Credential", "GCCredential"]
