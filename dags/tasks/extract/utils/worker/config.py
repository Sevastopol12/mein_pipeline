from tenacity import wait_exponential_jitter
from google.genai.errors import ClientError
from pydantic import BaseModel
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ExtractorConfig:
    model_name: str
    instruction: str
    response_schema: Optional[BaseModel] = None
    rpm: int = 15
    max_attempts: int = 10

    def _retry_on_exception(exception) -> bool:
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
