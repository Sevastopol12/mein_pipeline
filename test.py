import logging
import logging.config
import asyncio
from dags.tasks import run_extract

logging.basicConfig(
    level=logging.DEBUG,
    filename="pipe.log",
    filemode="w",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main():
    await run_extract()


if __name__ == "__main__":
    asyncio.run(main())
