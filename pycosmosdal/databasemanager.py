"""
The DatabaseManager class.
"""
from typing import Generator, Union

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.errors import DatabaseError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Database


class DatabaseManager(Manager):
    """
    This class is responsible for Database management.
    """

    def __init__(self, client: CosmosDbClient):
        """
        Creates a DatabaseManager instance.
        :param client: The client that is responsible for issuing commands to CosmosDb.
        """
        super().__init__(client)

    def create_database(self, database_id: str):
        """
        Creates a new database.
        :param database_id: The database id.
        """
        try:
            self.client.native_client.CreateDatabase({"id": database_id})
        except HTTPFailure as e:
            raise DatabaseError(e)

    def delete_database(self, database_id: str):
        """
        Deletes a database.
        :param database_id: The database id.
        """
        try:
            self.client.native_client.DeleteDatabase(
                DatabaseManager.get_database_link(database_id)
            )
        except HTTPFailure as e:
            raise DatabaseError(e)

    def list_databases(self) -> Generator[Database, None, None]:
        """
        Gets a list of databases.
        :return A generator that can be iterated to get the Database instances.
        :rtype: Generator[Database]
        """
        for database in self.client.native_client.ReadDatabases():
            yield Database(native_resource=database)

    def get_database(self, database_id: str) -> Union[Database, None]:
        """
        Gets a database by id
        :param database_id: The database id.
        :return: A Database instance else None if the database isn't found.
        :rtype: Database
        """
        query = dict(
            query="SELECT * FROM r WHERE r.id=@id",
            parameters=[dict(name="@id", value=database_id)],
        )

        databases = list(self.client.native_client.QueryDatabases(query))

        if len(databases) > 0:
            return Database(native_resource=databases[0])

        return None

    @staticmethod
    def get_database_link(database_id: str) -> str:
        """
        A helper method that gets a database's link given the database id.
        :param database_id: The database id.
        :return: The database's link.
        :rtype: str
        """
        return f"dbs/{database_id}"
