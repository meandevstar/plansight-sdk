# plansight_sdk

A simple database client SDK supporting user and activity CRUD operations.

## What This SDK Does

The `plansight_sdk` provides a simple interface for interacting with a SQLite database. It supports basic CRUD (Create, Read, Update, Delete) operations for user and activity data. The SDK includes:

- A `DatabaseClient` class for managing database connections and transactions.
- Methods for creating, reading, updating, and deleting records in the database.
- Validation to ensure data integrity and prevent SQL injection.
- Support for ordering query results.

## Installation

To install the SDK, use `pip`:

```bash
pip install plansight_sdk
```

## Usage

### Setting Up the Database
Before using the SDK, you need to set up the database schema, use the `setup_tables` method of the DatabaseClient class. Here's an example:

```python
from plansight_sdk import DatabaseClient

db_config = ':memory:'  # use an in-memory database for testing
db_client = DatabaseClient(db_config, 'sqlite')

db_client.connect()
db_client.setup_tables()
db_client.close()
```

### Creating Records
To create a new record in the database, use the `create_record` method of the DatabaseClient class. Here's an example:

```python
from plansight_sdk import DatabaseClient

data = {
    'name': 'John Doe',
    'email': 'john.doe@example.com',
    'created_at': '2023-05-01 12:00:00'
}

record_id = db_client.create_record('Users', data)
print(f"Record created with ID: {record_id}")
```

### Reading Records
To read records from the database, use the `read_records` method. You can specify filters and ordering as needed:

```python
from plansight_sdk import DatabaseClient

filters = {'email': 'john.doe@example.com'}
order_by = {'created_at': 'DESC'}

records = db_client.read_records('Users', filters=filters, order_by=order_by)
for record in records:
    print(record)
```

### Updating Records
To update an existing record, use the `update_record` method. Specify the table, the data to update, and the filters to identify the record:

```python
from plansight_sdk import DatabaseClient

update_data = {'name': 'Johnathan Doe'}
filters = {'email': 'john.doe@example.com'}

db_client.update_record('Users', update_data, filters)
print("Record updated successfully.")
```

### Deleting Records
To delete records from the database, use the `delete_records` method. Specify the table and the filters to identify the records to delete:

```python
from plansight_sdk import DatabaseClient

filters = {'email': 'john.doe@example.com'}

db_client.delete_records('Users', filters)
print("Record(s) deleted successfully.")
```

## Running Tests

### Unit Tests
To run the tests for the SDK, use `pytest`. Ensure you have `pytest` installed:
```bash
pip install pytest
```

Then, run the tests:
```bash
pytest -s -v
```
This will automatically discover and run all the test files that match the pattern test_*.py or *_test.py.

### Real World Tests
There is a sample script for real world usage - `test_sdk.py`. Test the package by installing it locally:

```bash
pip install -e .
python test_sdk.py
```

## Design Patterns

### Adapter Pattern:

The DatabaseAdapter class follows the Adapter design pattern. It abstracts the underlying database operations, providing a consistent interface for the DatabaseClient to interact with the database. This pattern allows for easy substitution of different database backends if needed in the future.

### Context Manager Pattern:

The DatabaseClient class implements the context manager protocol (__enter__ and __exit__ methods). This pattern ensures that database connections are properly managed, with automatic handling of transactions and resource cleanup. It provides a clean and concise way to manage database transactions using the with statement.

### Factory Pattern (Implicit):

Although not explicitly implemented, the initialization of the DatabaseAdapter within the DatabaseClient can be seen as a simple form of the Factory pattern. The DatabaseClient creates an instance of the DatabaseAdapter based on the provided database configuration.


## Contributing
Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Author
meandevstar