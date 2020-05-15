from typing import Generator

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.errors import ContainerError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Container


class ContainerManager(Manager):
    def __init__(self, client: CosmosDbClient):
        super().__init__(client)

    def create_container(self, container_id: str, database_id: str, **kwargs):
        parameter_dict = dict(id=container_id)
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
            raise ContainerError(e)

    def delete_container(self, container_id: str, database_id: str):
        try:
            self.client.native_client.DeleteContainer(
                ContainerManager.get_container_link(container_id, database_id)
            )
        except HTTPFailure as e:
            raise ContainerError(e)

    def list_containers(self, database_id: str) -> Generator[Container, None, None]:
        for container in self.client.native_client.ReadContainers(
            DatabaseManager.get_database_link(database_id)
        ):
            yield Container(native_resource=container)

    def get_container(self, container_id: str, database_id: str) -> Container:
        try:
            container = self.client.native_client.ReadContainer(
                ContainerManager.get_container_link(container_id, database_id)
            )
            return Container(native_resource=container)
        except HTTPFailure as e:
            raise ContainerError(e)

    def find_container(self, container_id: str, database_id: str) -> Container:
        query = dict(
            query="SELECT * FROM r WHERE r.id=@id",
            parameters=[dict(name="@id", value=container_id)],
        )

        containers = list(
            self.client.native_client.QueryContainers(
                DatabaseManager.get_database_link(database_id), query
            )
        )

        if len(containers) > 0:
            return Container(native_resource=containers[0])

        return None

    @staticmethod
    def get_container_link(container_id: str, database_id: str):
        return f"{DatabaseManager.get_database_link(database_id)}/colls/{container_id}"
