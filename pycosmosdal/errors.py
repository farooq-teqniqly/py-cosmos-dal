from abc import ABC, abstractmethod
from azure.cosmos.errors import HTTPFailure

"""Errors that wrap CosmosDb HTTP errors."""


class CosmosDalError(Exception):
    """Base class for DAL errors"""

    def __init__(self, cosmos_error: HTTPFailure):
        """
        Creates an CosmosDalError instance. This method is intended to be called from derived classes.
        :param cosmos_error: The CosmosDb error to wrap.
        """
        self.status_code = cosmos_error.status_code
        self.message = cosmos_error._http_error_message


class DatabaseError(CosmosDalError):
    """Represents errors raised by the DatabaseManager."""

    def __init__(self, cosmos_error: HTTPFailure):
        """
        Creates a DatabaseError instance.
        :param cosmos_error: The CosmosDb error to wrap.
        """
        super().__init__(cosmos_error)


class CollectionError(CosmosDalError):
    """Represents errors raised by the CollectionManager."""

    def __init__(self, cosmos_error: HTTPFailure):
        """
        Creates a CollectionError instance.
        :param cosmos_error: The CosmosDb error to wrap.
        """
        super().__init__(cosmos_error)


class DocumentError(CosmosDalError):
    """Represents errors raised by the DocumentManager."""

    def __init__(self, cosmos_error: HTTPFailure):
        """
        Creates a DocumentError instance.
        :param cosmos_error: The CosmosDb error to wrap.
        """
        super().__init__(cosmos_error)
