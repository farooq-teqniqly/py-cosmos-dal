"""The Disposable class is used as a context manager for automatic cleanup of resources. This class allows
for the use of the 'with' statement."""
from typing import Any


class Disposable:
    def __init__(self, obj: Any):
        """
        Creates a Disposable instance.
        :param obj: The object that the Disposable instance will manage.
        """
        self._obj = obj

    def __enter__(self):
        return self._obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._obj = None
