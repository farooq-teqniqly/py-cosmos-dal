from unittest import TestCase

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import CollectionError

DATABASE_NAME = __name__

client = CosmosDbEmulatorClient()


class CollectionManagerTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)
        cls.database_manager.create_database(DATABASE_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.database_manager.delete_database(DATABASE_NAME)

    def setUp(self):
        self.collection_manager = CollectionManager(client)

    def test_create_list_delete_collection(self):
        try:
            self.collection_manager.create_collection("foobar", DATABASE_NAME)
            collections = list(self.collection_manager.list_collections(DATABASE_NAME))

            self.assertEqual(1, len(collections))
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_create_collection_when_exists_raises_error(self):
        try:
            self.collection_manager.create_collection("foobar", DATABASE_NAME)
            self.assertRaises(
                CollectionError,
                self.collection_manager.create_collection,
                "foobar",
                DATABASE_NAME,
            )
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_create_collection_with_unique_keys(self):
        try:
            self.collection_manager.create_collection(
                "foobar",
                DATABASE_NAME,
                unique_keys=[dict(paths=["/field1/field2", "/field3"])],
            )
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_create_collection_specify_resource_units(self):
        try:
            self.collection_manager.create_collection(
                "foobar", DATABASE_NAME, throughput=1000
            )
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_create_collection_with_partition(self):
        try:
            self.collection_manager.create_collection(
                "foobar", DATABASE_NAME, partition_key=dict(paths=["/field1"])
            )
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_list_collections_when_none_returns_empty_list(self):
        self.assertEqual(
            0, len(list(self.collection_manager.list_collections(DATABASE_NAME)))
        )

    def test_delete_collection_when_not_exists_raises_ContainerError(self):
        self.assertRaises(
            CollectionError, self.collection_manager.delete_collection, "foo", "bar"
        )

    def test_get_collection_returns_collection(self):
        try:
            self.collection_manager.create_collection("foobar", DATABASE_NAME)
            collection = self.collection_manager.get_collection("foobar", DATABASE_NAME)
            self.assertEqual("foobar", collection.resource_id)
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_get_collection_when_none_raises_ContainerError(self):
        self.assertRaises(
            CollectionError, self.collection_manager.get_collection, "foo", "bar"
        )

    def test_find_collection_returns_collection(self):
        try:
            self.collection_manager.create_collection("foobar", DATABASE_NAME)
            collection = self.collection_manager.find_collection(
                "foobar", DATABASE_NAME
            )
            self.assertEqual("foobar", collection.resource_id)
        finally:
            self.collection_manager.delete_collection("foobar", DATABASE_NAME)

    def test_find_collection_when_none_returns_None(self):
        self.assertIsNone(
            self.collection_manager.find_collection("foobar", DATABASE_NAME)
        )
