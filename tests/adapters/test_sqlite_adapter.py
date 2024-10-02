import pytest
import unittest
import os
from plansight_sdk.adapters.sqlite_adapter import SQLiteAdapter
from plansight_sdk.exceptions import DatabaseError, ConstraintViolationError

@pytest.mark.skip(reason="Not implemented")
class TestSQLiteAdapter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tests/test_db.sqlite'
        cls._create_test_db()

    @classmethod
    def execute_statements(cls, statements):
        """Run multiple SQL statements"""

        for statement in statements.split(';'):
            statement = statement.strip()
            if statement:
                cls.adapter.execute(statement)

        cls.adapter.commit()

    @classmethod
    def _create_test_db(cls):
        schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'plansight_sdk', 'schema', 'schema.sql')
        with open(schema_path, 'r') as file:
            schema_sql = file.read()
        
        cls.adapter = SQLiteAdapter()
        cls.adapter.connect(cls.db_path)
        cls.execute_statements(schema_sql)

    @classmethod
    def tearDownClass(cls):
        cls.adapter.close()

    def setUp(self):
        self.adapter = TestSQLiteAdapter.adapter

    def tearDown(self):
        self.execute_statements('DELETE FROM Users;')

    def test_connect_and_close(self):
        adapter = SQLiteAdapter()
        adapter.connect(':memory:')
        self.assertIsNotNone(adapter.conn)
        self.assertIsNotNone(adapter.cursor)
        adapter.close()
        self.assertIsNone(adapter.conn)
        self.assertIsNone(adapter.cursor)

    def test_execute_and_fetchall(self):
        self.execute_statements('''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Alice Smith', 'alice@example.com', '2023-01-15 10:00:00');
        ''')

        self.adapter.execute('SELECT * FROM Users WHERE name = ?', ('Alice Smith',))
        results = self.adapter.fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], 'Alice Smith')

    def test_commit(self):
        self.execute_statements('''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Bob Jones', 'bob@example.com', '2023-02-20 14:30:00');
        ''')

        self.adapter.execute('SELECT * FROM Users WHERE name = ?', ('Bob Jones',))
        results = self.adapter.fetchall()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], 'Bob Jones')

    def test_rollback(self):
        try:
            self.adapter.execute('''
                INSERT INTO Users (name, email, created_at) VALUES
                ('Carol Lee', 'carol@example.com', '2023-03-10 09:15:00');
            ''')
            raise Exception("Force rollback")
        except:
            self.adapter.rollback()

        self.adapter.execute('SELECT * FROM Users WHERE name = ?', ('Carol Lee',))
        results = self.adapter.fetchall()
        self.assertEqual(len(results), 0)

    def test_constraint_violation_error(self):
        self.execute_statements('''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Alice Smith', 'alice@example.com', '2023-01-15 10:00:00');
        ''')

        with self.assertRaises(ConstraintViolationError):
            self.execute_statements('''
                INSERT INTO Users (name, email, created_at) VALUES
                ('Duplicate Alice', 'alice@example.com', '2023-01-15 10:00:00');
            ''')

    def test_database_error(self):
        with self.assertRaises(DatabaseError):
            self.adapter.execute('SELECT * FROM NonExistentTable')
