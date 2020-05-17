from abc import ABC

from pycosmosdal.cosmosdbclient import CosmosDbClient

"""
The Manager class is a base class for CosmosDb resource managers.
"""


class Manager(ABC):
    """
    A base class for managers. A manager is responsible for interfacing
    with CosmosDb resources.
    """

    def __init__(self, client: CosmosDbClient):
        """
        Creates a Manager instance. This method is intended to be called by
        derived classes.
        :param client: The client that is responsible for issuing commands to CosmosDb.
        """
        self.client = client
