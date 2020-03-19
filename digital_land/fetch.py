import os.path
import hashlib
import shutil
import requests
import functools

cache_dir = "./var/cache"


def cache_path(url, filename=None):
    if not filename:
        filename = hashlib.sha256(url.encode("utf-8")).hexdigest()

    return os.path.join(cache_dir, filename)


def fetch(url, filename=None, path=None):
    if not path:
        path = cache_path(url, filename)

    if not os.path.isfile(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        response = requests.get(url, stream=True)
        response.raw.read = functools.partial(response.raw.read, decode_content=True)
        with open(path, "wb") as f:
            shutil.copyfileobj(response.raw, f)

    return path
