from unittest import TestCase

from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.disposable import Disposable
from pycosmosdal.errors import DatabaseError

client = CosmosDbEmulatorClient()


class DatabaseManagerTests(TestCase):
    def test_create_list_delete_database(self):
        with Disposable(DatabaseManager(client)) as c:
            try:
                c.create_database("foo")
                databases = list(c.list_databases())
                self.assertEqual(1, len(databases))
                self.assertEqual("foo", databases[0].id)
            finally:
                c.delete_database("foo")

    def test_list_databases_when_none_returns_empty_list(self):
        with Disposable(DatabaseManager(client)) as c:
            self.assertEqual(0, len(list(c.list_databases())))

    def test_create_database_when_exists_raises_DatabaseError(self):
        with Disposable(DatabaseManager(client)) as c:
            try:
                c.create_database("foo")
                self.assertRaises(DatabaseError, c.create_database, "foo")
            finally:
                c.delete_database("foo")

    def test_delete_database_when_not_exists_raises_DatabaseError(self):
        with Disposable(DatabaseManager(client)) as c:
            self.assertRaises(DatabaseError, c.delete_database, "foo")

    def test_find_database_returns_database(self):
        with Disposable(DatabaseManager(client)) as c:
            try:
                c.create_database("foo")
                database = c.find_database("foo")
                self.assertEqual("foo", database.id)
            finally:
                c.delete_database("foo")

    def test_find_database_when_not_exists_returns_none(self):
        with Disposable(DatabaseManager(client)) as c:
            self.assertIsNone(c.find_database("foo"))
