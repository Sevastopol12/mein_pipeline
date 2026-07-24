import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Database configuration loaded from environment variables.
    """

    dbo_url = os.getenv("SUPABASE_URL")
    dbo_key = os.getenv("SUPABASE_KEY")

    @property
    def connection_url(self) -> str:
        """
        Constructs the async SQLAlchemy connection URL.

        Returns:
            str: The formatted connection string.
        """
        return {"url": self.dbo_url, "key": self.dbo_key}


config = DatabaseConfig()
