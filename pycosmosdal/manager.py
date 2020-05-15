from abc import ABC

from pycosmosdal.cosmosdbclient import CosmosDbClient


class Manager(ABC):
    def __init__(self, client: CosmosDbClient):
        self.client = client
