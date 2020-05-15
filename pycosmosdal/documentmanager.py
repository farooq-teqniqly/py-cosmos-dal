from typing import Any, Generator

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.containermanager import ContainerManager
from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.errors import DocumentError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Document


class DocumentManager(Manager):
    def __init__(self, client: CosmosDbClient):
        super().__init__(client)

    def create_document(
        self, document: dict, collection_id: str, database_id: str
    ) -> Document:
        document = self.client.native_client.CreateItem(
            ContainerManager.get_container_link(collection_id, database_id), document
        )

        return Document(document)

    def get_document(self, document_id: Any, collection_id: str, database_id: str):
        document = self.client.native_client.ReadItem(
            DocumentManager.get_document_link(document_id, collection_id, database_id)
        )
        return Document(document)

    def delete_document(self, document_id: Any, collection_id: str, database_id: str):
        try:
            self.client.native_client.DeleteItem(
                DocumentManager.get_document_link(
                    document_id, collection_id, database_id
                )
            )
        except HTTPFailure as e:
            raise DocumentError(e)

    def get_documents(
        self, collection_id: str, database_id: str,
    ) -> Generator[Document, None, None]:

        for document in self.client.native_client.ReadItems(
            ContainerManager.get_container_link(collection_id, database_id)
        ):
            yield Document(document)

    @staticmethod
    def get_document_link(
        document_id: Any, collection_id: str, database_id: str
    ) -> str:
        return f"{ContainerManager.get_container_link(collection_id, database_id)}/docs/{str(document_id)}"
