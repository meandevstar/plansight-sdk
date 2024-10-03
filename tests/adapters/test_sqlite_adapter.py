import os
import pytest
from plansight_sdk.adapters.sqlite_adapter import SQLiteAdapter
from plansight_sdk.exceptions import DatabaseError, ConstraintViolationError

@pytest.fixture(scope='session')
def adapter():
    """Fixture to set up and tear down the SQLite adapter."""

    db_path = 'tests/test_db.sqlite'
    schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'plansight_sdk', 'schema', 'schema.sql')
    with open(schema_path, 'r') as file:
        schema_sql = file.read()

    adapter = SQLiteAdapter()
    adapter.connect(db_path)

    execute_statements(adapter, schema_sql)

    yield adapter

    adapter.close()

def execute_statements(adapter, statements):
    """Run multiple SQL statements."""
    for statement in statements.split(';'):
        statement = statement.strip()
        if statement:
            adapter.execute(statement)
    adapter.commit()

@pytest.fixture(autouse=True)
def setup_and_teardown_data(adapter):
    """Fixture to insert initial data before each test and clean up after each test."""

    initial_data_sql = '''
        INSERT INTO Users (name, email, created_at) VALUES
        ('Alice Smith', 'alice@example.com', '2023-01-15 10:00:00'),
        ('Bob Jones', 'bob@example.com', '2023-02-20 14:30:00');
    '''
    execute_statements(adapter, initial_data_sql)

    yield

    cleanup_sql = '''
        DELETE FROM Users;
    '''
    execute_statements(adapter, cleanup_sql)

def test_connect_and_close():
    adapter = SQLiteAdapter()
    adapter.connect(':memory:')
    assert adapter.conn is not None
    assert adapter.cursor is not None
    adapter.close()
    assert adapter.conn is None
    assert adapter.cursor is None

def test_execute_and_fetchall(adapter):
    adapter.execute('''
        INSERT INTO Users (name, email, created_at) VALUES
        ('Carol Lee', 'carol@example.com', '2023-03-10 09:15:00');
    ''')
    adapter.commit()

    adapter.execute('SELECT * FROM Users WHERE name = ?', ('Carol Lee',))
    results = adapter.fetchall()
    assert len(results) == 1
    assert results[0][1] == 'Carol Lee'

def test_commit(adapter):
    adapter.execute('''
        INSERT INTO Users (name, email, created_at) VALUES
        ('David Brown', 'david@example.com', '2023-04-25 11:00:00');
    ''')
    adapter.commit()

    adapter.execute('SELECT * FROM Users WHERE name = ?', ('David Brown',))
    results = adapter.fetchall()
    assert len(results) == 1
    assert results[0][1] == 'David Brown'

def test_rollback(adapter):
    try:
        adapter.execute('''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Eve White', 'eve@example.com', '2023-05-01 12:00:00');
        ''')
        raise Exception("Force rollback")
    except:
        adapter.rollback()

    adapter.execute('SELECT * FROM Users WHERE name = ?', ('Eve White',))
    results = adapter.fetchall()
    assert len(results) == 0

def test_constraint_violation_error(adapter):
    with pytest.raises(ConstraintViolationError):
        adapter.execute('''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Duplicate Alice', 'alice@example.com', '2023-01-15 10:00:00');
        ''')
        adapter.commit()

def test_database_error(adapter):
    with pytest.raises(DatabaseError):
        adapter.execute('SELECT * FROM NonExistentTable')