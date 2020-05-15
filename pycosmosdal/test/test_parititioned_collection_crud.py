from unittest.case import TestCase

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.documentmanager import DocumentManager
from pycosmosdal.errors import DocumentError
from pycosmosdal.test.test_documentmanager import DocumentManagerTests

DATABASE_NAME = __name__
PARTITIONED_COLLECTION_NAME = f"{DATABASE_NAME}_container_partitioned"

client = CosmosDbEmulatorClient()


class PartitionedCollectionCrudTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)
        cls.database_manager.create_database(DATABASE_NAME)

        cls.collection_manager = CollectionManager(client)

        cls.collection_manager.create_collection(
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
            partition_key=dict(paths=["/id"]),
        )

    @classmethod
    def tearDownClass(cls):
        cls.database_manager.delete_database(DATABASE_NAME)

    def setUp(self):
        self.document_manager = DocumentManager(client)

    def test_partitioned_collection_delete_document_when_partition_not_specified_raises_DocumentError(
        self,
    ):
        document_id = str(1)

        self.document_manager.upsert_document(
            DocumentManagerTests.get_test_document(document_id),
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
        )

        self.assertRaises(
            DocumentError,
            self.document_manager.delete_document,
            document_id,
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
        )

    def test_partitioned_collection_delete_document(self):
        document_id = str(1)

        self.document_manager.upsert_document(
            DocumentManagerTests.get_test_document(document_id),
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
        )

        self.document_manager.delete_document(
            document_id,
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
            partition_key=document_id,
        )

    def test_partitioned_collection_cross_partition_query_when_disabled_raises_DocumentError(
        self,
    ):
        document_id = str(1)

        self.document_manager.upsert_document(
            DocumentManagerTests.get_test_document(document_id),
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
        )

        query_result = self.document_manager.query_documents(
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
            "SELECT * FROM r WHERE r.purchase_order_number=@po_number",
            [dict(name="@po_number", value="PO18009186470")],
        )

        self.assertRaises(DocumentError, query_result.fetch_next)

    def test_partitioned_collection_cross_partition_query(self,):
        document_id = str(1)

        self.document_manager.upsert_document(
            DocumentManagerTests.get_test_document(document_id),
            PARTITIONED_COLLECTION_NAME,
            DATABASE_NAME,
        )

        document = list(
            self.document_manager.query_documents(
                PARTITIONED_COLLECTION_NAME,
                DATABASE_NAME,
                "SELECT * FROM r WHERE r.purchase_order_number=@po_number",
                [dict(name="@po_number", value="PO18009186470")],
                enable_cross_partition_query=True,
            ).fetch_next()
        )[0]

        self.assertEqual(document_id, document.resource_id)
