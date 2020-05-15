from abc import ABC
from typing import Any

from azure.cosmos.query_iterable import QueryIterable


class CosmosResource(ABC):
    def __init__(self, native_resource: Any):
        self.resource_id = native_resource["id"]
        self.native_resource = native_resource


class Database(CosmosResource):
    def __init__(self, native_resource: Any):
        super().__init__(native_resource)


class Collection(CosmosResource):
    def __init__(self, native_resource: Any):
        super().__init__(native_resource)


class Document(CosmosResource):
    def __init__(self, native_resource: Any):
        super().__init__(native_resource)


class DocumentQueryResults:
    def __init__(self, query_iterable: QueryIterable):
        self._query_iterable = query_iterable

    def fetch_next(self) -> list:
        return self._query_iterable.fetch_next_block()
