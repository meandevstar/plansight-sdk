import os
import pytest
from plansight_sdk.database_client import DatabaseClient
from plansight_sdk.exceptions import DatabaseError, ValidationError, ConstraintViolationError

@pytest.fixture(scope='session')
def db_client():
    """Fixture to set up and tear down the database client."""
    db_path = 'tests/test_db.sqlite'
    db_config = db_path
    db_type = 'sqlite'

    schema_path = os.path.join(os.path.dirname(__file__), '..', 'plansight_sdk', 'schema', 'schema.sql')
    with open(schema_path, 'r') as file:
        schema_sql = file.read()

    db_client = DatabaseClient(db_config, db_type)
    db_client.__enter__()

    db_client.execute_statements(db_client, schema_sql)

    yield db_client

    db_client.__exit__(None, None, None)

@pytest.fixture(autouse=True)
def setup_and_teardown_data(db_client):
    """Fixture to insert initial data before each test and clean up after each test."""
    # Insert initial data for testing
    initial_data_sql = '''
        INSERT INTO Users (name, email, created_at) VALUES
        ('Alice Smith', 'alice@example.com', '2023-01-15 10:00:00'),
        ('Bob Jones', 'bob@example.com', '2023-02-20 14:30:00'),
        ('Carol Lee', 'carol@example.com', '2023-03-10 09:15:00'),
        ('David Brown', 'david@example.com', '2023-04-01 12:00:00'),
        ('Eve White', 'eve@example.com', '2023-05-01 14:00:00');

        INSERT INTO Activities (user_id, activity_type, timestamp, metadata) VALUES
        (1, 'Login', '2023-04-01 08:00:00', '{"ip": "192.168.1.2"}'),
        (1, 'Purchase', '2023-04-01 09:30:00', '{"item_id": 555, "amount": 29.99}'),
        (1, 'Logout', '2023-04-02 17:45:00', '{"ip": "192.168.1.3"}'),
        (3, 'Login', '2023-04-03 10:00:00', '{"ip": "192.168.1.4"}'),
        (3, 'Logout', '2023-04-05 10:00:00', '{"ip": "192.168.1.4"}'),
        (4, 'Login', '2023-04-05 13:00:00', '{"ip": "192.168.1.5"}'),
        (6, 'Login', '2023-04-05 09:00:00', '{"ip": "192.168.1.6"}');
    '''
    db_client.execute_statements(db_client, initial_data_sql)

    yield

    # Clean up data after each test
    cleanup_sql = '''
        DELETE FROM Users;
        DELETE FROM Activities;
    '''
    db_client.execute_statements(db_client, cleanup_sql)

def test_create_record_success(db_client):
    data = {
        'name': 'John Doe',
        'email': 'john@example.com',
        'created_at': '2023-06-01 15:00:00'
    }
    user_id = db_client.create_record('Users', data)
    assert user_id is not None

def test_create_record_validation_error(db_client):
    data = {
        'name': 'John Doe' * 20,
        'email': 'john@example.com',
        'created_at': '2023-06-01 15:00:00'
    }
    with pytest.raises(ValidationError) as excinfo:
        db_client.create_record('Users', data)
    assert str(excinfo.value) == "Field name exceeds maximum length of 100 characters"

def test_create_record_constraint_violation(db_client):
    data = {
        'name': 'Alice Smith',
        'email': 'alice@example.com', # duplicated email
        'created_at': '2023-05-01 12:00:00'
    }
    with pytest.raises(ConstraintViolationError) as excinfo:
        db_client.create_record('Users', data)
    assert str(excinfo.value) == "UNIQUE constraint failed: Users.email"

def test_read_records_success(db_client):
    filters = {'name': 'Alice Smith'}
    records = db_client.read_records('Users', filters)
    assert len(records) == 1
    assert records[0][1] == 'Alice Smith'

def test_update_records_success(db_client):
    updates = {'email': 'alice_new@example.com'}
    filters = {'name': 'Alice Smith'}
    db_client.update_records('Users', updates, filters)
    records = db_client.read_records('Users', filters)
    assert len(records) == 1
    assert records[0][2] == 'alice_new@example.com'

def test_update_records_validation_error(db_client):
    updates = {'email': 'alice_new@example.com' * 10}
    filters = {'name': 'Alice Smith'}
    with pytest.raises(ValidationError) as excinfo:
        db_client.update_records('Users', updates, filters)
    assert str(excinfo.value) == "Field email exceeds maximum length of 100 characters"

def test_delete_records_success(db_client):
    filters = {'name': 'Alice Smith'}
    db_client.delete_records('Users', filters)
    records = db_client.read_records('Users', filters)
    assert len(records) == 0

def test_transaction_commit(db_client):
    data = {
        'name': 'John Doe',
        'email': 'john@example.com',
        'created_at': '2023-06-01 15:00:00'
    }
    with DatabaseClient(db_client.db_config, db_client.db_type) as temp_client:
        user_id = temp_client.create_record('Users', data)
        assert user_id is not None
    records = db_client.read_records('Users', {'email': 'john@example.com'})
    assert len(records) == 1

def test_transaction_rollback(db_client):
    user_data = {
        'name': 'John Doe',
        'email': 'john@example.com',
        'created_at': '2023-06-01 15:00:00'
    }
    activity_data = {
        'activity_type': 'Login',
        'timestamp': '2023-06-01 15:00:00',
        'metadata': '{"ip": "192.168.1.7"}'
    }
    try:
        with DatabaseClient(db_client.db_config, db_client.db_type) as temp_client:
            user_id = temp_client.create_record('Users', user_data)
            assert user_id is not None
            activity_data['user_id'] = user_id
            activity_id = temp_client.create_record('Activities', activity_data)
            assert activity_id is not None
            raise Exception("Force rollback")
    except:
        pass

    records = db_client.read_records('Users', {'email': 'john@example.com'})
    assert len(records) == 0
    records = db_client.read_records('Activities', {'user_id': activity_data['user_id']})
    assert len(records) == 0

def test_read_records_no_filters(db_client):
    records = db_client.read_records('Users')
    assert len(records) >= 5

def test_read_records_with_ordering(db_client):
    order_by = {'created_at': 'DESC'}
    records = db_client.read_records('Users', order_by=order_by)
    assert records[0][1] == 'Eve White'

def test_read_records_with_limit(db_client):
    records = db_client.read_records('Users', limit=2)
    assert len(records) == 2

def test_read_records_with_pagination(db_client):
    order_by = {'created_at': 'DESC'}
    records = db_client.read_records('Users', order_by=order_by, limit=2, offset=2)
    assert len(records) == 2
    # check that the records are the third and fourth records in the table
    all_records = db_client.read_records('Users')
    assert records[0] == all_records[2]
    assert records[1] == all_records[1]
