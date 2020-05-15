from datetime import date
from unittest import TestCase

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.documentmanager import DocumentManager
from pycosmosdal.errors import DocumentError

DATABASE_NAME = __name__
COLLECTION_NAME = f"{DATABASE_NAME}_container"

client = CosmosDbEmulatorClient()


class DocumentManagerTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)
        cls.database_manager.create_database(DATABASE_NAME)

        cls.container_manager = CollectionManager(client)
        cls.container_manager.create_collection(COLLECTION_NAME, DATABASE_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.database_manager.delete_database(DATABASE_NAME)

    def setUp(self):
        self.document_manager = DocumentManager(client)

    def test_create_get_delete_document(self):
        try:
            self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            document = self.document_manager.get_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )
            self.assertEqual("foobar", document.resource_id)
        finally:
            self.document_manager.delete_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )

    def test_upsert_document(self):
        try:
            self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            document = self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            self.assertEqual("foobar", document.resource_id)
        finally:
            self.document_manager.delete_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )

    def test_get_documents(self):
        try:
            self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            documents = list(
                self.document_manager.get_documents(COLLECTION_NAME, DATABASE_NAME)
            )

            self.assertEqual(1, len(documents))
        finally:
            self.document_manager.delete_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )

    def test_delete_non_existent_document_raises_DocumentError(self):
        self.assertRaises(
            DocumentError, self.document_manager.delete_document, "foo", "bar", "baz"
        )

    def test_query_documents(self):
        try:
            self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            query_result = self.document_manager.query_documents(
                COLLECTION_NAME, DATABASE_NAME, "SELECT * FROM r WHERE r.id='foobar'",
            )

            self.assertEqual(1, len(query_result.fetch_next()))
        finally:
            self.document_manager.delete_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )

    def test_query_documents_limit_results(self):
        try:
            for i in range(0, 10):
                self.document_manager.upsert_document(
                    DocumentManagerTests.get_test_document(f"foobar-{i}"),
                    COLLECTION_NAME,
                    DATABASE_NAME,
                )

            query_result = self.document_manager.query_documents(
                COLLECTION_NAME,
                DATABASE_NAME,
                "SELECT * FROM r WHERE r.id>'0'",
                max_item_count=3,
            )

            documents: list = query_result.fetch_next()
            self.assertEqual(3, len(documents))
        finally:
            for i in range(0, 10):
                self.document_manager.delete_document(
                    f"foobar-{i}", COLLECTION_NAME, DATABASE_NAME
                )

    def test_query_documents_with_parameters(self):
        try:
            self.document_manager.upsert_document(
                DocumentManagerTests.get_test_document("foobar"),
                COLLECTION_NAME,
                DATABASE_NAME,
            )

            query_result = self.document_manager.query_documents(
                COLLECTION_NAME,
                DATABASE_NAME,
                "SELECT * FROM r WHERE r.subtotal>@subtotal AND EXISTS(SELECT VALUE n FROM n in r.items WHERE n.product_id=@product_id)",
                [
                    dict(name="@subtotal", value=400),
                    dict(name="@product_id", value=100),
                ],
            )

            self.assertEqual(1, len(query_result.fetch_next()))
        finally:
            self.document_manager.delete_document(
                "foobar", COLLECTION_NAME, DATABASE_NAME
            )

    @staticmethod
    def get_test_document(document_id: str) -> dict:
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
