from abc import ABC, abstractmethod
from azure.cosmos.errors import HTTPFailure


class CosmosDalError(Exception):
    @abstractmethod
    def __init__(self, cosmos_error: HTTPFailure):
        self.status_code = cosmos_error.status_code
        self.message = cosmos_error._http_error_message


class DatabaseError(CosmosDalError):
    def __init__(self, cosmos_error: HTTPFailure):
        super().__init__(cosmos_error)


class ContainerError(CosmosDalError):
    def __init__(self, cosmos_error: HTTPFailure):
        super().__init__(cosmos_error)
