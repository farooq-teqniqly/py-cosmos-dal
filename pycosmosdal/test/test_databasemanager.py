from unittest import TestCase

from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import DatabaseError

client = CosmosDbEmulatorClient()


class DatabaseManagerTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)

    def test_create_list_delete_database(self):
        try:
            self.database_manager.create_database("foo")
            databases = list(self.database_manager.list_databases())
            self.assertEqual(1, len(databases))
            self.assertEqual("foo", databases[0].resource_id)
        finally:
            self.database_manager.delete_database("foo")

    def test_list_databases_when_none_returns_empty_list(self):
        self.assertEqual(0, len(list(self.database_manager.list_databases())))

    def test_create_database_when_exists_raises_DatabaseError(self):
        try:
            self.database_manager.create_database("foo")
            self.assertRaises(
                DatabaseError, self.database_manager.create_database, "foo"
            )
        finally:
            self.database_manager.delete_database("foo")

    def test_delete_database_when_not_exists_raises_DatabaseError(self):
        self.assertRaises(DatabaseError, self.database_manager.delete_database, "foo")

    def test_find_database_returns_database(self):
        try:
            self.database_manager.create_database("foo")
            database = self.database_manager.find_database("foo")
            self.assertEqual("foo", database.resource_id)
        finally:
            self.database_manager.delete_database("foo")

    def test_find_database_when_not_exists_returns_none(self):
        self.assertIsNone(self.database_manager.find_database("foo"))
