from abc import ABC
from typing import Any


class CosmosResource(ABC):
    def __init__(self, native_resource: Any):
        self.resource_id = native_resource["id"]
        self.native_resource = native_resource


class Database(CosmosResource):
    def __init__(self, native_resource: Any):
        super().__init__(native_resource)


class Container(CosmosResource):
    def __init__(self, native_resource: Any):
        super().__init__(native_resource)
