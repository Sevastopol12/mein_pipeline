from tenacity import wait_exponential_jitter
from google.genai.errors import ClientError, ServerError
from pydantic import BaseModel
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractorConfig:
    model_name: str
    instruction: str
    response_schema: BaseModel
    rpm: int = 15
    max_attempts: int = 10

    def _retry_on_exception(self, exception) -> bool:
        if isinstance(exception, ServerError):
            return True

        if isinstance(exception, ClientError):
            if exception.code != 429:
                return True

        return False


class ResponseBaseWait:
    def __init__(self):
        self.default_wait = wait_exponential_jitter(initial=2, max=60)

    def __call__(self, retry_state):
        exception = retry_state.outcome.exception()

        if exception.code == 429:
            return 60
        if exception.code == 503:
            return 4
        if exception.code == 504:
            return 15
        return self.default_wait(retry_state=retry_state)
