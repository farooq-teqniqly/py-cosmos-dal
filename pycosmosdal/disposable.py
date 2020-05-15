from typing import Any


class Disposable:
    def __init__(self, obj: Any):
        self._obj = obj

    def __enter__(self):
        return self._obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._obj = None
