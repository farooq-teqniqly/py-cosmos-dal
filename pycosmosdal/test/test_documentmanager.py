from datetime import date
from unittest import TestCase

from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.containermanager import ContainerManager
from pycosmosdal.documentmanager import DocumentManager
from pycosmosdal.errors import DocumentError

DATABASE_NAME = __name__
CONTAINER_NAME = f"{DATABASE_NAME}_container"

client = CosmosDbEmulatorClient()


class DocumentManagerTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)
        cls.database_manager.create_database(DATABASE_NAME)

        cls.container_manager = ContainerManager(client)
        cls.container_manager.create_container(CONTAINER_NAME, DATABASE_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.database_manager.delete_database(DATABASE_NAME)

    def setUp(self):
        self.document_manager = DocumentManager(client)

    def test_create_get_delete_document(self):
        try:
            self.document_manager.create_document(
                DocumentManagerTests._get_test_document("foobar"),
                CONTAINER_NAME,
                DATABASE_NAME,
            )

            document = self.document_manager.get_document(
                "foobar", CONTAINER_NAME, DATABASE_NAME
            )
            self.assertEqual("foobar", document.resource_id)
        finally:
            self.document_manager.delete_document(
                "foobar", CONTAINER_NAME, DATABASE_NAME
            )

    def test_get_documents(self):
        try:
            self.document_manager.create_document(
                DocumentManagerTests._get_test_document("foobar"),
                CONTAINER_NAME,
                DATABASE_NAME,
            )

            documents = list(
                self.document_manager.get_documents(CONTAINER_NAME, DATABASE_NAME)
            )

            self.assertEqual(1, len(documents))
        finally:
            self.document_manager.delete_document(
                "foobar", CONTAINER_NAME, DATABASE_NAME
            )

    def test_delete_non_existent_document_raises_DocumentError(self):
        self.assertRaises(
            DocumentError, self.document_manager.delete_document, "foo", "bar", "baz"
        )

    @staticmethod
    def _get_test_document(document_id: str) -> dict:
        return {
            "id": document_id,
            "account_number": "Account1",
            "purchase_order_number": "PO18009186470",
            "order_date": date(2020, 5, 14).strftime("%c"),
            "subtotal": 419.4589,
            "tax_amount": 12.5838,
            "freight": 472.3108,
            "total_due": 985.018,
            "items": [
                {
                    "order_qty": 1,
                    "product_id": 100,
                    "unit_price": 418.4589,
                    "line_price": 418.4589,
                }
            ],
            "ttl": 60 * 60 * 24 * 30,
        }
