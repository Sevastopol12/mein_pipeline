import logging

from .config import ExtractorConfig, ResponseBaseWait
from ..queue import PoisonPill, ProductionQueue

from pydantic import ValidationError

from google.genai.types import GenerateContentConfig, GenerateContentResponse, Part
from google.genai.errors import ClientError, ServerError

from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
)

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
                    logger.info("No task left. Returning...")
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

            except (ClientError, ServerError, ValidationError) as e:
                logger.critical(e)
                task_status = {"status": "FAILED", "object": task}
                task_holder.record(task_status)
            except Exception as e:
                logger.exception(e)
                task_status = {"status": "FAILED", "object": task}
                task_holder.record(task_status)
            finally:
                task_holder.queue.task_done()

    async def extract_content(
        self,
        task: dict,
    ) -> bytes:

        file = Part.from_bytes(
            data=task.get("raw_bytes"), mime_type=f"application/{task.get('format')}"
        )

        logger.info(f"Format: {task.get('format')}.\n File: {file}")

        response: GenerateContentResponse = await self.client.models.generate_content(
            model=self.config.model_name,
            contents=[self.config.instruction, file],
            config=GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=self.config.response_schema,
            ),
        )
        content = response.text.replace("*", "").replace("`", "").strip()

        return self.config.response_schema.model_validate_json(content)


__all__ = ["Extractor"]
