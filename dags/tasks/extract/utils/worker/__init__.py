import logging

from .config import ExtractorConfig, ResponseBaseWait
from ..queue import PoisonPill

from pydantic import ValidationError

from asyncio import Queue
from google.genai.types import File, GenerateContentConfig, GenerateContentResponse
from google.genai.errors import ClientError, ServerError

from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
)

from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


async def extractor(
    pending_queue: Queue[File | PoisonPill], client, config: ExtractorConfig
):
    rpm_limit = AsyncLimiter(config.rpm)

    resolved: list[bytes] = []
    unresolved: list[File] = []

    while True:
        try:
            task = await pending_queue.get()

            if isinstance(task, PoisonPill):
                logger.info("No task left. Returning...")
                return {"resolved": resolved, "unresolved": unresolved}

            logger.info(f"{config.model_name} got file: {task.name}")
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(config.max_attempts),
                retry=retry_if_exception(config._retry_on_exception),
                wait=ResponseBaseWait(),
            ):
                with attempt:
                    async with rpm_limit:
                        json_response_object = await _get_content(task, client, config)
                    resolved.append(json_response_object)
                    logger.info(f"[{task.name}] content extracted.")

        except (ClientError, ServerError, ValidationError) as e:
            logger.critical(e)
            unresolved.append(task)

        except Exception:
            logger.error("Max attempt reached. Returning...")
            unresolved.append(task)

        finally:
            pending_queue.task_done()


async def _get_content(
    file: File,
    client,
    config,
) -> bytes:
    response: GenerateContentResponse = await client.models.generate_content(
        model=config.model_name,
        contents=[config.instruction, file],
        config=GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=config.response_schema,
        ),
    )
    content = response.text.replace("*", "").replace("`", "").strip()

    if not config.response_schema.model_validate_json(content):
        raise ValidationError()

    return content


__all__ = ["extractor"]
