import base64
from .worker import ExtractorConfig, ResponseBaseWait, Extractor
from .queue import ProductionQueue


def decode_and_convert_to_records(file_format: list[str], encoded_bytes: list[str]):
    storage = []
    for file_fmt, encoded_data in zip(file_format, encoded_bytes):
        raw_bytes = base64.b64decode(encoded_data)

        storage.append({"format": file_fmt, "raw_bytes": raw_bytes})

    return storage


__all__ = ["ExtractorConfig", "ResponseBaseWait", "ProductionQueue", "Extractor", "decode_and_convert_to_records"]
