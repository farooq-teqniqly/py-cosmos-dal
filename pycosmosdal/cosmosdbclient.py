"""
The CosmosDbClient class.
"""
from azure.cosmos.cosmos_client import CosmosClient


class CosmosDbClient:
    """
    The CosmosDbClient class serves as a wrapper around the CosmosClient.
    """

    def __init__(self, host: str, master_key: str):
        """
        Creates a CosmosDbClient instance.
        :param host: The CosmosDb host url.
        :param master_key: The CosmosDb access key.
        """
        self._client = CosmosClient(host, {"masterKey": master_key})

    @property
    def native_client(self):
        """
        The wrapped CosmosClient. Managers use this property to send commands to CosmosDb.
        :return: The wrapped CosmosClient.
        :rtype: CosmosClient
        """
        return self._client


class CosmosDbEmulatorClient(CosmosDbClient):
    """
    The CosmosDbEmulatorClient client is used when issuing commands
    """

    def __init__(self):
        super().__init__(
            "https://localhost:8081",
            "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
        )
