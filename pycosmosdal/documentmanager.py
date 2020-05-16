"""
The DocumentManager class.
"""
from typing import Any, Generator, Dict, List

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.errors import DocumentError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Document, DocumentQueryResults


class DocumentManager(Manager):
    """
    This class is responsible for Document management and querying.
    """

    def __init__(self, client: CosmosDbClient):
        """
        Creates a DocumentManager instance.
        :param client: The client that is responsible for issuing commands to CosmosDb.
        """
        super().__init__(client)

    def upsert_document(
        self, document: dict, collection_id: str, database_id: str
    ) -> Document:
        """
        Inserts a new document or if the document exists, updates the document.
        :param document: The document to upsert.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :return: A Document instance which wraps a CosmosDb document.
        ":rtype: Document
        """
        try:
            document = self.client.native_client.UpsertItem(
                CollectionManager.get_collection_link(collection_id, database_id),
                document,
            )
            return Document(document)
        except HTTPFailure as e:
            raise DocumentError(e)

    def get_document(self, document_id: Any, collection_id: str, database_id: str):
        """
        Gets a document by its id.
        :param document_id: The document id.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :return: A Document instance which wraps a CosmosDb document.
        ":rtype: Document
        """
        try:
            document = self.client.native_client.ReadItem(
                DocumentManager.get_document_link(
                    document_id, collection_id, database_id
                )
            )
            return Document(document)
        except HTTPFailure as e:
            raise DocumentError(e)

    def delete_document(
        self, document_id: Any, collection_id: str, database_id: str, **kwargs
    ):
        """
        Deletes a document by its id.
        :param document_id: The document id.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :param kwargs: Delete options:
            partition_key: This must be specified when deleting from a partitioned collection.
        """
        options = dict()
        partition_key = kwargs.get("partition_key")

        if partition_key:
            options["partitionKey"] = partition_key

        try:
            self.client.native_client.DeleteItem(
                DocumentManager.get_document_link(
                    document_id, collection_id, database_id
                ),
                options=options,
            )
        except HTTPFailure as e:
            raise DocumentError(e)

    def get_documents(
        self, collection_id: str, database_id: str, **kwargs
    ) -> DocumentQueryResults:
        """
        Get all documents in a collection. It is advised that the max_item_count option is passed with a value
        other than -1 to reduce the chance of CosmosDb throttling errors.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :param kwargs: Get document options:
            max_item_count: This controls the maximum number of documents retrieved in a single call to
            DocumentQueryResults.fetch_next().
        :return: A DocumentQueryResults instance which can be iterated through by calling
        DocumentQueryResults.fetch_next()
        :rtype: DocumentQueryResults
        """
        options = dict(maxItemCount=-1)

        max_item_count = kwargs.get("max_item_count")

        if max_item_count:
            options["maxItemCount"] = int(max_item_count)

        try:
            query_iterable = self.client.native_client.ReadItems(
                CollectionManager.get_collection_link(collection_id, database_id),
                feed_options=options,
            )

            return DocumentQueryResults(query_iterable)
        except HTTPFailure as e:
            raise DocumentError(e)

    def query_documents(
        self,
        collection_id: str,
        database_id: str,
        query: str,
        query_parameters: List[Dict[str, Any]] = None,
        **kwargs,
    ) -> DocumentQueryResults:
        """
        Get documents based on a SQL query. It is advised that the max_item_count option is passed with a value
        other than -1 to reduce the chance of CosmosDb throttling errors.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :param query: The SQL query.
        :param query_parameters: If the SQL query is parameterized, the parameter names and values are specified here.
        :param kwargs: Query options:
            max_item_count: This controls the maximum number of documents retrieved in a single call to
            DocumentQueryResults.fetch_next().

            partition_key: When querying a partitioned collection, this parameter specifies the partition key to query.

            enable_cross_partition_query: When set to True, this query will work across multiple partitions.

        :return: A DocumentQueryResults instance which can be iterated through by calling
        DocumentQueryResults.fetch_next()
        :rtype: DocumentQueryResults
        """
        query_spec = dict(query=query)

        if query_parameters:
            query_spec["parameters"] = query_parameters

        options = dict()

        max_item_count = kwargs.get("max_item_count")

        if max_item_count:
            options["maxItemCount"] = int(max_item_count)

        partition_key = kwargs.get("partition_key")

        if partition_key:
            options["partitionKey"] = partition_key

        enable_cross_partition_query = kwargs.get("enable_cross_partition_query")

        if enable_cross_partition_query:
            options["enableCrossPartitionQuery"] = bool(enable_cross_partition_query)

        try:
            query_iterable = self.client.native_client.QueryItems(
                CollectionManager.get_collection_link(collection_id, database_id),
                query_spec,
                options=options,
            )

            return DocumentQueryResults(query_iterable)
        except HTTPFailure as e:
            raise DocumentError(e)

    @staticmethod
    def get_document_link(
        document_id: Any, collection_id: str, database_id: str
    ) -> str:
        """
        A helper method that gets a document's link given the collection id and database id.
        :param document_id: The document id.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :return: The document's link.
        :rtype: str
        """
        return f"{CollectionManager.get_collection_link(collection_id, database_id)}/docs/{str(document_id)}"
