from typing import Any, Generator, Dict, List

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.errors import DocumentError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Document, DocumentQueryResults


class DocumentManager(Manager):
    def __init__(self, client: CosmosDbClient):
        super().__init__(client)

    def upsert_document(
        self, document: dict, collection_id: str, database_id: str
    ) -> Document:
        try:
            document = self.client.native_client.UpsertItem(
                CollectionManager.get_collection_link(collection_id, database_id),
                document,
            )
            return Document(document)
        except HTTPFailure as e:
            raise DocumentError(e)

    def get_document(self, document_id: Any, collection_id: str, database_id: str):
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
        self, collection_id: str, database_id: str,
    ) -> Generator[Document, None, None]:
        try:
            for document in self.client.native_client.ReadItems(
                CollectionManager.get_collection_link(collection_id, database_id)
            ):
                yield Document(document)
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
        return f"{CollectionManager.get_collection_link(collection_id, database_id)}/docs/{str(document_id)}"
