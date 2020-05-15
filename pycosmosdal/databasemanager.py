from typing import Generator

from azure.cosmos.errors import HTTPFailure

from pycosmosdal.cosmosdbclient import CosmosDbClient
from pycosmosdal.errors import DatabaseError
from pycosmosdal.manager import Manager
from pycosmosdal.models import Database


class DatabaseManager(Manager):
    def __init__(self, client: CosmosDbClient):
        super().__init__(client)

    def create_database(self, database_id: str):
        try:
            self.client.native_client.CreateDatabase({"id": database_id})
        except HTTPFailure as e:
            raise DatabaseError(e)

    def delete_database(self, database_id: str):
        try:
            self.client.native_client.DeleteDatabase(
                DatabaseManager.get_database_link(database_id)
            )
        except HTTPFailure as e:
            raise DatabaseError(e)

    def list_databases(self) -> Generator[Database, None, None]:
        for database in self.client.native_client.ReadDatabases():
            yield Database(id=database["id"], native_db=database)

    def find_database(self, database_id: str) -> Database:
        query = dict(
            query="SELECT * FROM r WHERE r.id=@id",
            parameters=[dict(name="@id", value=database_id)],
        )

        databases = list(self.client.native_client.QueryDatabases(query))

        if len(databases) > 0:
            return Database(id=databases[0]["id"], native_db=databases[0])

        return None

    @staticmethod
    def get_database_link(database_id: str) -> str:
        return f"dbs/{database_id}"
