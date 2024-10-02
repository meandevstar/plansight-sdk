import unittest
import os
import pytest
from plansight_sdk.database_client import DatabaseClient
from plansight_sdk.exceptions import DatabaseError, ValidationError, ConstraintViolationError

class TestDatabaseClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_path = 'tests/test_db.sqlite'
        cls.db_config = cls.db_path
        cls.db_type = 'sqlite'
        cls._create_test_db()

    @classmethod
    def execute_statements(cls, statements):
        """Run multiple SQL statements"""

        for statement in statements.split(';'):
            statement = statement.strip()
            if statement:
                cls.db_client.adapter.execute(statement)

        cls.db_client.adapter.commit()

    @classmethod
    def _create_test_db(cls):
        print('==> called create test db')

        schema_path = os.path.join(os.path.dirname(__file__), '..', 'plansight_sdk', 'schema', 'schema.sql')
        with open(schema_path, 'r') as file:
            schema_sql = file.read()
        
        cls.db_client = DatabaseClient(cls.db_config, cls.db_type)
        cls.db_client.__enter__()
        cls.execute_statements(schema_sql)
        cls.execute_statements(
            '''
            INSERT INTO Users (name, email, created_at) VALUES
            ('Alice Smith', 'alice@example.com', '2023-01-15 10:00:00'),
            ('Bob Jones', 'bob@example.com', '2023-02-20 14:30:00'),
            ('Carol Lee', 'carol@example.com', '2023-03-10 09:15:00');

            INSERT INTO Activities (user_id, activity_type, timestamp, metadata) VALUES
            (1, 'Login', '2023-04-01 08:00:00', '{"ip": "192.1681.2"}'),
            (1, 'Purchase', '2023-04-01 09:30:00', '{"item_id": 555, "amount": 29.99}'),
            (2, 'Logout', '2023-04-02 17:45:00', '{"ip": "192.168.1.3"}');
            '''
        )

    @classmethod
    def tearDownClass(cls):
        cls.db_client.__exit__(None, None, None)

    def setUp(self):
        self.db_client = TestDatabaseClient.db_client

    def tearDown(self):
        self.execute_statements(
            """
            DELETE FROM Users;
            DELETE FROM Activities;
            """
        )

    @pytest.mark.skip(reason="Not implemented")
    def test_create_record_success(self):
        data = {
            'name': 'David Brown',
            'email': 'david@example.com',
            'created_at': '2023-05-01 12:00:00'
        }
        user_id = self.db_client.create_record('Users', data)
        self.assertIsNotNone(user_id)

    @pytest.mark.skip(reason="Not implemented")
    def test_create_record_validation_error(self):
        data = {
            'name': 'David Brown' * 10,
            'email': 'david@example.com',
            'created_at': '2023-05-01 12:00:00'
        }
        with self.assertRaises(ValidationError) as context:
            self.db_client.create_record('Users', data)
        self.assertEqual(str(context.exception), "Field name exceeds maximum length of 100 characters")

    @pytest.mark.skip(reason="Not implemented")
    def test_create_record_constraint_violation(self):
        data = {
            'name': 'Alice Smith',
            'email': 'alice@example.com', 
            'created_at': '2023-05-01 12:00:00'
        }
        with self.assertRaises(ConstraintViolationError) as context:
            self.db_client.create_record('Users', data)
        self.assertEqual(str(context.exception), "UNIQUE constraint failed: Users.email")

    @pytest.mark.skip(reason="Not implemented")
    def test_read_records_success(self):
        filters = {'name': 'Alice Smith'}
        records = self.db_client.read_records('Users', filters)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][1], 'Alice Smith')

    @pytest.mark.skip(reason="Not implemented")
    def test_update_records_success(self):
        updates = {'email': 'alice_new@example.com'}
        filters = {'name': 'Alice Smith'}
        self.db_client.update_records('Users', updates, filters)
        records = self.db_client.read_records('Users', filters)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][2], 'alice_new@example.com')

    @pytest.mark.skip(reason="Not implemented")
    def test_update_records_validation_error(self):
        updates = {'email': 'alice_new@example.com' * 10}
        filters = {'name': 'Alice Smith'}
        with self.assertRaises(ValidationError) as context:
            self.db_client.update_records('Users', updates, filters)
        self.assertEqual(str(context.exception), "Field email exceeds maximum length of 100 characters")

    @pytest.mark.skip(reason="Not implemented")
    def test_delete_records_success(self):
        filters = {'name': 'Alice Smith'}
        self.db_client.delete_records('Users', filters)
        records = self.db_client.read_records('Users', filters)
        self.assertEqual(len(records), 0)

    @pytest.mark.skip(reason="Not implemented")
    def test_transaction_commit(self):
        data = {
            'name': 'Eve White',
            'email': 'eve@example.com',
            'created_at': '2023-06-01 15:00:00'
        }
        with DatabaseClient(self.db_config, self.db_type) as db_client:
            user_id = db_client.create_record('Users', data)
            self.assertIsNotNone(user_id)
        records = self.db_client.read_records('Users', {'name': 'Eve White'})
        self.assertEqual(len(records), 1)

    def test_transaction_rollback(self):
        data = {
            'name': 'Eve White',
            'email': 'eve@example.com',
            'created_at': '2023-06-01 15:00:00'
        }
        try:
            with DatabaseClient(self.db_config, self.db_type) as db_client:
                user_id = db_client.create_record('Users', data)
                self.assertIsNotNone(user_id)
                raise Exception("Force rollback")
        except:    
            pass
        records = self.db_client.read_records('Users', {'email': 'eve@example.com'})
        print('==> after records', records)
        self.assertEqual(len(records), 0)

    def test_read_records_no_filters(self):
        records = self.db_client.read_records('Users')
        self.assertGreaterEqual(len(records), 3)

    def test_read_records_with_ordering(self):
        records = self.db_client.read_records('Users', order_by='created_at DESC')
        self.assertEqual(records[0][1], 'Carol Lee')

    def test_read_records_with_limit(self):
        records = self.db_client.read_records('Users')
        print('==> records', records)
        records = self.db_client.read_records('Users', limit=2)
        print('==> records', records)
        self.assertEqual(len(records), 2)
