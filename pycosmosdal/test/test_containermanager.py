from unittest import TestCase

from pycosmosdal.containermanager import ContainerManager
from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import ContainerError

DATABASE_NAME = __name__

client = CosmosDbEmulatorClient()


class ContainerManagerTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_manager = DatabaseManager(client)
        cls.database_manager.create_database(DATABASE_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.database_manager.delete_database(DATABASE_NAME)

    def setUp(self):
        self.container_manager = ContainerManager(client)

    def test_create_list_delete_container(self):
        try:
            self.container_manager.create_container("foobar", DATABASE_NAME)
            containers = list(self.container_manager.list_containers(DATABASE_NAME))

            self.assertEqual(1, len(containers))
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_create_container_when_exists_raises_error(self):
        try:
            self.container_manager.create_container("foobar", DATABASE_NAME)
            self.assertRaises(
                ContainerError,
                self.container_manager.create_container,
                "foobar",
                DATABASE_NAME,
            )
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_create_container_with_unique_keys(self):
        try:
            self.container_manager.create_container(
                "foobar",
                DATABASE_NAME,
                unique_keys=[dict(paths=["/field1/field2", "/field3"])],
            )
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_create_container_specify_resource_units(self):
        try:
            self.container_manager.create_container(
                "foobar", DATABASE_NAME, throughput=1000
            )
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_create_container_with_partition(self):
        try:
            self.container_manager.create_container(
                "foobar", DATABASE_NAME, partition_key=dict(paths=["/field1"])
            )
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_list_containers_when_none_returns_empty_list(self):
        self.assertEqual(
            0, len(list(self.container_manager.list_containers(DATABASE_NAME)))
        )

    def test_delete_container_when_not_exists_raises_ContainerError(self):
        self.assertRaises(
            ContainerError, self.container_manager.delete_container, "foo", "bar"
        )

    def test_get_container_returns_container(self):
        try:
            self.container_manager.create_container("foobar", DATABASE_NAME)
            container = self.container_manager.get_container("foobar", DATABASE_NAME)
            self.assertEqual("foobar", container.resource_id)
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_get_container_when_none_raises_ContainerError(self):
        self.assertRaises(
            ContainerError, self.container_manager.get_container, "foo", "bar"
        )

    def test_find_container_returns_container(self):
        try:
            self.container_manager.create_container("foobar", DATABASE_NAME)
            container = self.container_manager.find_container("foobar", DATABASE_NAME)
            self.assertEqual("foobar", container.resource_id)
        finally:
            self.container_manager.delete_container("foobar", DATABASE_NAME)

    def test_find_container_when_none_returns_None(self):
        self.assertIsNone(
            self.container_manager.find_container("foobar", DATABASE_NAME)
        )
