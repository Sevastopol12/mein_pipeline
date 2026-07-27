import base64
from hashlib import sha256


def hash_file_content(file: str | bytes) -> str:
    if isinstance(file, str):
        hash_object = sha256()

        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_object.update(chunk)
    else:
        hash_object = sha256(file)

    digest = hash_object.hexdigest()

    del hash_object
    return base64.b64encode(digest.encode("utf-8")).decode("utf-8")


__all__ = ["hash_file_content"]
