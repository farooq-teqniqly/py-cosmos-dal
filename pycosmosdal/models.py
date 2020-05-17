"""
Models serving as wrappers around CosmosDb resources.
"""
from abc import ABC
from typing import Any

from azure.cosmos.errors import HTTPFailure
from azure.cosmos.query_iterable import QueryIterable

from pycosmosdal.errors import DocumentError


class CosmosResource(ABC):
    """The base class for objects representing CosmosDb resources."""

    def __init__(self, native_resource: Any):
        """
        Instantiates a CosmosResource. This method is intended to be called
        by derived classes only.

        :param native_resource:
            The CosmosDb resource to wrap.
        """
        self.resource_id = native_resource["id"]
        self.native_resource = native_resource


class Database(CosmosResource):
    """Represents a CosmosDb database."""

    def __init__(self, native_resource: Any):
        """
        Creates a Database instance.
        :param native_resource: The CosmosDb database to wrap.
        """
        super().__init__(native_resource)


class Collection(CosmosResource):
    """Represents a CosmosDb collection."""

    def __init__(self, native_resource: Any):
        """
        Creates a Collection instance.
        :param native_resource: The CosmosDb collection to wrap.
        """
        super().__init__(native_resource)


class Document(CosmosResource):
    """Represents a CosmosDb Document."""

    def __init__(self, native_resource: Any):
        """
        Creates a Document instance.
        :param native_resource: The CosmosDb document to wrap.
        """
        super().__init__(native_resource)


class DocumentQueryResults:
    """Represents the results of a CosmosDb Document query.
    This class is a wrapper around a QueryIterable."""

    def __init__(self, query_iterable: QueryIterable):
        """
        Creates a DocumentQueryResults instance.
        :param query_iterable: The CosmosDb QueryIterable to wrap.
        """
        self._query_iterable = query_iterable

    def fetch_next(self) -> list:
        """
        Gets the next block of documents from the query result.
        :return: The list of results. If all the results have been read,
        a zero length list is returned.
        :rtype: list
        """
        try:
            return [Document(d) for d in self._query_iterable.fetch_next_block()]
        except HTTPFailure as e:
            raise DocumentError(e)
