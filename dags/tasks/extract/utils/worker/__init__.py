import logging

from .config import (
    ExtractorConfig,
    ResponseBaseWait,
    FetchFailed,
    UnsupportedFileFormat,
)
from ..queue import PoisonPill, ProductionQueue

from pydantic import ValidationError

from google.genai.types import GenerateContentConfig, GenerateContentResponse, Part
from google.genai.errors import ClientError, ServerError

from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, RetryError
from asyncio import timeout
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


class Extractor:
    def __init__(self, client, config: ExtractorConfig):
        self.client = client
        self.config = config

    async def process(self, task_holder: ProductionQueue) -> None:
        rpm_limit = AsyncLimiter(self.config.rpm)

        while True:
            try:
                task: dict = await task_holder.queue.get()

                if isinstance(task, PoisonPill):
                    logger.info(
                        f"No task left. Returning...[{self.config.model_name}]."
                    )
                    return

                logger.info(f"{self.config.model_name} got file: {task.get('format')}")

                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(self.config.max_attempts),
                    retry=retry_if_exception(self.config._retry_on_exception),
                    wait=ResponseBaseWait(),
                ):
                    with attempt:
                        async with rpm_limit:
                            json_response_object = await self.extract_content(task)
                        task_status = {
                            "status": "SUCCEED",
                            "object": json_response_object,
                        }
                        task_holder.record(task_status)

            except RetryError as e:
                logger.exception(f"[{self.config.model_name}]: {e}")

                last_exception = e.last_attempt.exception()

                if isinstance(last_exception, ClientError):
                    task_holder.assign_for_review()

                elif isinstance(last_exception, ServerError) or isinstance(
                    last_exception, ValidationError
                ):
                    task_holder.assign_for_rerun()

                task_holder.record(
                    {"status": "FAILED", "object": task, "exc": last_exception}
                )

            except Exception as e:
                logger.exception(e)
                task_holder.assign_for_review()
                task_holder.record({"status": "FAILED", "object": task})

            finally:
                task_holder.queue.task_done()

    async def extract_content(
        self,
        task: dict,
    ) -> bytes:

        mime_type = self.config.map_mime_type(
            file_format=task.get("format").replace(".", "")
        )
        if mime_type is None:
            raise UnsupportedFileFormat

        file = Part.from_bytes(data=task.get("raw_bytes"), mime_type=f"{mime_type}")

        logger.info(f"[{self.config.model_name}]: Extracting content...")

        async with timeout(self.config.max_time_wait_on_task):
            response: GenerateContentResponse = (
                await self.client.models.generate_content(
                    model=self.config.model_name,
                    contents=[self.config.instruction, file],
                    config=GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=self.config.response_schema,
                    ),
                )
            )
        logger.info(
            f"[{self.config.model_name}]: Extract complete. Got: [{type(response)}]"
        )
        if response.text is None:
            raise FetchFailed

        content = response.text.replace("*", "").replace("`", "").strip()

        return self.config.response_schema.model_validate_json(content)


__all__ = ["Extractor"]
