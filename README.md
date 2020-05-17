# PyCosmosDal

PyCosmosDal is a wrapper around the Microsoft Azure CosmosDb Python SDK. It's goal is to reduce the amount of code
necessary to work with CosmosDb. It provides a simple object model that wraps CosmosDb resources such as 
Documents, Collections, and Databases. Operations can also be called using only resource id's rather than deriving the
links CosmosDb requires.

## Concepts

### Managers
PyCosmosDal uses *Managers* to interact with CosmosDb. There are managers for databases, collections, and documents. To 
create a database, use a ```DatabaseManager```. Use a ```CollectionManager``` to create a collection. Use a ```DocumentManager```
to create documents.

### Querying
When using the native CosmosDb Python SDK, query results are returned in a ```QueryIterable``` instance. The iteration
needs to be invoked in order to get results, i.e. the query is evaluated lazily. PyCosmosDal wraps the ```QueryIterable``` in a ```DocumentQueryResults``` instance.
Call the ```fetch_next``` method to retrieve the results.

**Note:** It is advised to specify the ```max_item_count``` option when querying do reduce the chance of CosmosDb throttling
the request.

## Example
The example below creates a database, partitioned collection, two documents, and queries for documents. The CosmosDb emulator
needs to be running in order for this example to work.  

```python
from datetime import date

from pycosmosdal.collectionmanager import CollectionManager
from pycosmosdal.cosmosdbclient import CosmosDbEmulatorClient
from pycosmosdal.databasemanager import DatabaseManager
from pycosmosdal.disposable import Disposable
from pycosmosdal.documentmanager import DocumentManager

from pycosmosdal.errors import CosmosDalError
from pycosmosdal.models import Document

database_id = "my_db"
collection_id = "my_collection"

# Create a client that uses the CosmosDb emulator
with Disposable(CosmosDbEmulatorClient()) as client:
    # Create database
    database_manager = DatabaseManager(client)
    database_manager.create_database(database_id)

    # Create partitioned collection
    collection_manager = CollectionManager(client)

    collection_manager.create_collection(
        collection_id, database_id, partition_key=dict(paths=["/id"])
    )

    document_manager = DocumentManager(client)

    documents = [
        {
            "id": "doc-001",
            "created": date(2020, 5, 16).strftime("%c"),
            "owner_id": "user-123",
        },
        {
            "id": "doc-002",
            "created": date(2020, 5, 12).strftime("%c"),
            "owner_id": "user-456",
        },
    ]

    # Add documents
    try:
        for document in documents:
            document_manager.upsert_document(document, collection_id, database_id)

        # Retrieve all the documents
        query_result = document_manager.get_documents(collection_id, database_id)
        all_documents = list(query_result.fetch_next())
        assert len(all_documents) == len(documents)

        # Query for a document by the partition key
        query_result = document_manager.query_documents(
            collection_id,
            database_id,
            "SELECT r.id FROM r WHERE r.id=@id",
            [dict(name="@id", value="doc-002")],
        )

        this_document: Document = list(query_result.fetch_next())[0]
        assert this_document.resource_id == "doc-002"

        # Query for a document by a non-partition key attribute. This means
        # cross-partition querying needs to be enabled in this request.
        query_result = document_manager.query_documents(
            collection_id,
            database_id,
            "SELECT r.id FROM r WHERE r.owner_id=@ownerId",
            [dict(name="@ownerId", value="user-456")],
            enable_cross_partition_query=True,
        )

        this_document: Document = list(query_result.fetch_next())[0]
        assert this_document.resource_id == "doc-002"
    except CosmosDalError as e:
        print(e)
    finally:
        # Delete the database
        database_manager.delete_database(database_id)
```

## Using an Azure CosmosDb Instance
To use an Azure CosmosDb instance just use the ```CosmosDbClient```:

```python
with Disposable(CosmosDbClient(host, key)) as client:
```

## More Examples
Review the tests for more comprehensive examples. 

## Running the tests

**Note:** To avoid incurring costs, run the tests against the **CosmosDb Emulator.**