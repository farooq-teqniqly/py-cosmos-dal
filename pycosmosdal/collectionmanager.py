from typing import Generator, Union

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import CollectionError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Collection


class CollectionManager(Manager):
    def __init__(self, client: CosmosDbClient):
        super().__init__(client)

    def create_collection(self, collection_id: str, database_id: str, **kwargs):
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
        try:
            self.client.native_client.DeleteContainer(
                CollectionManager.get_collection_link(collection_id, database_id)
            )
        except HTTPFailure as e:
            raise CollectionError(e)

    def list_collections(self, database_id: str) -> Generator[Collection, None, None]:
        for collection in self.client.native_client.ReadContainers(
            DatabaseManager.get_database_link(database_id)
        ):
            yield Collection(native_resource=collection)

    def get_collection(self, collection_id: str, database_id: str) -> Collection:
        try:
            collection = self.client.native_client.ReadContainer(
                CollectionManager.get_collection_link(collection_id, database_id)
            )
            return Collection(native_resource=collection)
        except HTTPFailure as e:
            raise CollectionError(e)

    def find_collection(
        self, collection_id: str, database_id: str
    ) -> Union[Collection, None]:
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
        return f"{DatabaseManager.get_database_link(database_id)}/colls/{collection_id}"
