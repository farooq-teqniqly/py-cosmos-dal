from azure.cosmos.cosmos_client import CosmosClient


class CosmosDbClient:
    def __init__(self, host: str, master_key: str):
        self._client = CosmosClient(host, {"masterKey": master_key})

    @property
    def native_client(self):
        return self._client


class CosmosDbEmulatorClient(CosmosDbClient):
    def __init__(self):
        super().__init__(
            "https://localhost:8081",
            "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
        )
