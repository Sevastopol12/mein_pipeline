import logging
from google.genai.types import File


logger = logging.getLogger(__name__)


def load_to_storage(results: list[dict]):
    try:
        resolved: list[str] = []
        unresolved: list[File] = []

        for worker_result in results:
            resolved.extend(worker_result["resolved"])
            unresolved.extend(worker_result["unresolved"])

        logger.debug(f"{type(resolved[0])}")
        data = "\n".join(content for content in resolved).encode("utf-8")
        
        with open("resolved.json", "wb") as file:
            file.write(data)
        
        logger.debug(f"stored: {data}")
    except Exception as e:
        logger.error(e)
        return
