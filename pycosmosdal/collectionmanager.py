"""
The CollectionManager class.
"""
from typing import Generator, Union

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import CollectionError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Collection


class CollectionManager(Manager):
    """
    This class is responsible for Collection management.
    """

    def __init__(self, client: CosmosDbClient):
        """
        Creates a CollectionManager instance.
        :param client: The client that is responsible for issuing commands to CosmosDb.
        """
        super().__init__(client)

    def create_collection(self, collection_id: str, database_id: str, **kwargs):
        """
        Creates a collection.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :param kwargs: Create options:
            unique_keys: The unique key policy is a dict that contains an array of paths.
            Example: unique_keys=[dict(paths=["/field1/field2", "/field3"])]

            partition_key: Creates a partitioned collection. This parameter accepts a dict that contains an array of paths.
            Example: partition_key=dict(paths=["/field1"])

            throughput: The number of throughput units. If not specified the collection is created with 400 units. Valid
            values are 400 - 10000.
        """
        parameter_dict = dict(id=collection_id)
        unique_key_policy = kwargs.get("unique_keys")

        if unique_key_policy:
            parameter_dict["uniqueKeyPolicy"] = dict(uniqueKeys=unique_key_policy)

        partition_key = kwargs.get("partition_key")

        if partition_key:
            parameter_dict["partitionKey"] = dict(
                paths=partition_key["paths"], kind="Hash", version=2
            )

        collection_options_dict = dict()
        throughput_units = kwargs.get("throughput")

        if throughput_units:
            collection_options_dict["offerThroughput"] = int(throughput_units)

        try:
            self.client.native_client.CreateContainer(
                DatabaseManager.get_database_link(database_id),
                parameter_dict,
                collection_options_dict,
            )
        except HTTPFailure as e:
            raise CollectionError(e)

    def delete_collection(self, collection_id: str, database_id: str):
        """
        Deletes a collection.
        :param collection_id: The collection id.
        :param database_id: The database id.
        """
        try:
            self.client.native_client.DeleteContainer(
                CollectionManager.get_collection_link(collection_id, database_id)
            )
        except HTTPFailure as e:
            raise CollectionError(e)

    def list_collections(self, database_id: str) -> Generator[Collection, None, None]:
        """
        Gets a list of collections.
        :return A generator that can be iterated to get the Collection instances.
        :rtype: Generator[Collection]
        """
        for collection in self.client.native_client.ReadContainers(
            DatabaseManager.get_database_link(database_id)
        ):
            yield Collection(native_resource=collection)

    def get_collection(
        self, collection_id: str, database_id: str
    ) -> Union[Collection, None]:
        """
        Gets a collection by id
        :param collection_id: The collection id.
        :param database_id: The database id.
        :return: A Collection instance else None if the collection isn't found.
        :rtype: Collection
        """
        query = dict(
            query="SELECT * FROM r WHERE r.id=@id",
            parameters=[dict(name="@id", value=collection_id)],
        )

        collections = list(
            self.client.native_client.QueryContainers(
                DatabaseManager.get_database_link(database_id), query
            )
        )

        if len(collections) > 0:
            return Collection(native_resource=collections[0])

        return None

    @staticmethod
    def get_collection_link(collection_id: str, database_id: str) -> str:
        """
        A helper method that gets a collection's link given the collection id and database id.
        :param collection_id: The collection id.
        :param database_id: The database id.
        :return: The collection's link.
        :rtype: str
        """
        return f"{DatabaseManager.get_database_link(database_id)}/colls/{collection_id}"
