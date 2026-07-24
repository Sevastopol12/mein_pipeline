import os
import logging
from google.genai import Client
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()


logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_async_client():
    client = Client(api_key=os.getenv("genai_key"))
    async with client.aio as async_client:
        yield async_client


__all__ = ["get_async_client"]
