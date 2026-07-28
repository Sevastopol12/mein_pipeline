import logging

from pydantic import BaseModel, ValidationError
from typing import Optional

from asyncio import Queue
from google.genai.types import File, GenerateContentConfig, GenerateContentResponse
from google.genai.errors import ClientError, ServerError

from tenacity import (
    AsyncRetrying,
    wait_exponential_jitter,
    retry_if_exception,
    stop_after_attempt,
)

from aiolimiter import AsyncLimiter


logger = logging.getLogger(__name__)


class PoisonPill:
    def __init__(self):
        pass


async def extract_file_contents(
    pending_queue: Queue[File | PoisonPill],
    client,
    model_name: str,
    instruction: str,
    response_schema: Optional[BaseModel],
    rpm: int = 15,
    max_attempts: int = 10,
):
    rpm_limit = AsyncLimiter(rpm)

    resolved: list[bytes] = []
    unresolved: list[File] = []

    while True:
        try:
            task = await pending_queue.get()

            if isinstance(task, PoisonPill):
                logger.info("No task left. Returning...")
                return {"resolved": resolved, "unresolved": unresolved}

            logger.debug(f"{model_name} got file: {task.name}")
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(max_attempts),
                retry=retry_if_exception(_retry),
                wait=ResponseBaseWait(),
            ):
                with attempt:
                    async with rpm_limit:
                        json_response_object = await _get_content(
                            task, client, model_name, instruction, response_schema
                        )
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
    model_name: str,
    instruction: str,
    response_schema: Optional[BaseModel],
) -> bytes:
    response: GenerateContentResponse = await client.models.generate_content(
        model=model_name,
        contents=[instruction, file],
        config=GenerateContentConfig(
            response_mime_type="application/json", response_schema=response_schema
        ),
    )
    content = response.text.replace("*", "").replace("`", "").strip()

    if not response_schema.model_validate_json(content):
        raise ValidationError()

    return content


def _retry(exception) -> bool:
    if isinstance(exception, ClientError) and exception.code != 429:
        return False
    return True


class ResponseBaseWait:
    def __init__(self):
        self.default_wait = wait_exponential_jitter(initial=2, max=60)

    def __call__(self, retry_state):
        exception = retry_state.outcome.exception()

        if exception.code == 429:
            return 60
        if exception.code == 503:
            return 4
        return self.default_wait(retry_state=retry_state)
