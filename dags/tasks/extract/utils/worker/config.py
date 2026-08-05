from tenacity import wait_exponential_jitter
from asyncio.exceptions import TimeoutError
from google.genai.errors import ClientError, ServerError
from pydantic import BaseModel
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExtractorConfig:
    model_name: str
    instruction: str
    response_schema: BaseModel
    rpm: int = 15
    max_attempts: int = 10
    max_time_wait_on_task: int = 180
    file_format_mapping: dict[str, str] = field(
        default_factory=lambda: {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
            "heic": "image/heic",
            "heif": "image/heif",
            "pdf": "application/pdf",
            "json": "text/plain",
            "md": "text/plain",
        }
    )

    def _retry_on_exception(self, exception) -> bool:
        if (
            isinstance(exception, ServerError)
            or isinstance(exception, TimeoutError)
            or isinstance(exception, FetchFailed)
        ):
            return True

        elif isinstance(exception, ClientError):
            if exception.code == 429:
                return True

        return False

    def map_mime_type(self, file_format: str):
        mime_type = self.file_format_mapping.get(file_format, None)

        return mime_type


class ResponseBaseWait:
    def __init__(self):
        self.default_wait = wait_exponential_jitter(initial=2, max=60)

    def __call__(self, retry_state):
        exception = retry_state.outcome.exception()
        if hasattr(exception, "code"):
            if exception.code == 429:
                return 60
            if exception.code == 503:
                return 4
            if exception.code == 504:
                return 15
        return self.default_wait(retry_state=retry_state)


class FetchFailed(Exception):
    """Raise when a worker return None type object on a fetch session."""

    pass


class UnsupportedFileFormat(Exception):
    """Raise when a worker tries to process unsupported-format file."""

    pass
