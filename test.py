import asyncio
import logging
from dotenv import load_dotenv
from dags.storage import ingestion
from dags.etl import run_etl


load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    filename="tester.log",
    filemode="w",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def test_run():
    await ingestion()
    await run_etl()


if __name__ == "__main__":
    asyncio.run(test_run())
