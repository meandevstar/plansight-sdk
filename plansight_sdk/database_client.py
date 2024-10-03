import re
import os
import threading
from .exceptions import DatabaseError, RecordNotFoundError, ValidationError, ConstraintViolationError
from .schema.schema_config import SCHEMA_CONFIG
from .adapters.sqlite_adapter import SQLiteAdapter

class DatabaseClient:
    def __init__(self, db_config, db_type='sqlite'):
        self.db_config = db_config
        self.db_type = db_type
        self.adapter = self._get_adapter()
        self.local = threading.local()

    def _get_adapter(self):
        if self.db_type == 'sqlite':
            return SQLiteAdapter()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def __enter__(self):
        self.adapter.connect(self.db_config)
        self.local.in_transaction = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.adapter.rollback()
        else:
            self.adapter.commit()
        self.adapter.close()
        self.local.in_transaction = False

    def _validate_data(self, table, data, optional=False):
        if table not in SCHEMA_CONFIG:
            raise ValidationError(f"Unknown table: {table}")

        config = SCHEMA_CONFIG[table]
        required_fields = config['required_fields']
        field_types = config['field_types']
        field_constraints = config.get('field_constraints', {})

        sql_injection_pattern = re.compile(r'(--|;|/\*|\*/|xp_)', re.IGNORECASE)
        
        if not optional:
            for field in required_fields:
                if field not in data:
                    raise ValidationError(f"Missing required field: {field}")
        
        for field, value in data.items():
            if field in field_types and not isinstance(value, field_types[field]):
                raise ValidationError(f"Incorrect type for field {field}: expected {field_types[field].__name__}, got {type(value).__name__}")
            if isinstance(value, str) and sql_injection_pattern.search(value):
                raise ValidationError(f"Potential SQL injection detected in field {field}")
            if field in field_constraints and len(value) > field_constraints[field]:
                raise ValidationError(f"Field {field} exceeds maximum length of {field_constraints[field]} characters")

    def _validate_filters(self, table, filters):
        if table not in SCHEMA_CONFIG:
            raise ValidationError(f"Unknown table: {table}")

        config = SCHEMA_CONFIG[table]
        field_types = config['field_types']

        sql_injection_pattern = re.compile(r'(--|;|/\*|\*/|xp_)', re.IGNORECASE)
        
        for field, value in filters.items():
            if field in field_types and not isinstance(value, field_types[field]):
                raise ValidationError(f"Incorrect type for filter {field}: expected {field_types[field].__name__}, got {type(value).__name__}")
            if isinstance(value, str) and sql_injection_pattern.search(value):
                raise ValidationError(f"Potential SQL injection detected in filter {field}")
            
    def _validate_order_by(self, table, order_by):
        if table not in SCHEMA_CONFIG:
            raise ValidationError(f"Unknown table: {table}")

        config = SCHEMA_CONFIG[table]
        field_types = config['field_types']

        for field, order in order_by.items():
            if field not in field_types:
                raise ValidationError(f"Unknown field in order_by: {field}")
            if order.upper() not in ['ASC', 'DESC']:
                raise ValidationError(f"Invalid sort order for field {field}: {order}")
            
    def connect(self):
        """Connect to the database."""
        self.adapter.connect(self.db_config)
        return self

    def close(self):
        """Close the database connection."""
        self.adapter.close()
            
    def execute_statements(self, statements):
        """Run multiple SQL statements."""
        for statement in statements.split(';'):
            statement = statement.strip()
            if statement:
                self.adapter.execute(statement)
        self.adapter.commit()

    def setup_tables(self):
        """
        Set up the necessary tables in the database.
        """
        schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'schema.sql')
        with open(schema_path, 'r') as file:
            schema_sql = file.read()

        self.execute_statements(schema_sql)

    def create_record(self, table, data):
        """
        Create a new record in the specified table.

        Args:
            table (str): The name of the table.
            data (Dict[str, Any]): The data to insert into the table.

        Returns:
            int: The ID of the created record.
        """

        self._validate_data(table, data)

        keys = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({keys}) VALUES ({placeholders});"

        try:
            self.adapter.execute(query, tuple(data.values()))
            if not getattr(self.local, 'in_transaction', False):
                self.adapter.commit()

            return self.adapter.cursor.lastrowid
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)

    def read_records(self, table, filters=None, order_by=None, limit=None, offset=None):
        """
        Read records from the specified table.

        Args:
            table (str): The name of the table.
            filters (Dict[str, Any], optional): The filters for the query. Defaults to None.
            order_by (Dict[str, str], optional): The ordering for the query. Defaults to None.
            limit (int, optional): The limit for the query. Defaults to None.
            offset (int, optional): The offset for the query. Defaults to None.

        Returns:
            List[tuple]: The fetched records.
        """

        if filters:
            self._validate_filters(table, filters)

        query = f"SELECT * FROM {table}"
        params = []

        if filters:
            filter_clauses = ' AND '.join([f"{k} = ?" for k in filters.keys()])
            query += f" WHERE {filter_clauses}"
            params.extend(filters.values())

        if order_by:
            order_clauses = ', '.join([f"{k} {v}" for k, v in order_by.items()])
            query += f" ORDER BY {order_clauses}"

        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"

        self.adapter.execute(query, tuple(params))
        return self.adapter.fetchall()

    def update_records(self, table, updates, filters):
        """
        Update records in the specified table.

        Args:
            table (str): The name of the table.
            data (Dict[str, Any]): The data to update in the table.
            filters (Dict[str, Any]): The filters to identify the records to update.
        """

        if not updates:
            raise ValidationError("No updates provided")
        if not filters:
            raise ValidationError("No filters provided")

        self._validate_data(table, updates, optional=True)
        self._validate_filters(table, filters)

        update_clauses = ', '.join([f"{k} = ?" for k in updates.keys()])
        filter_clauses = ' AND '.join([f"{k} = ?" for k in filters.keys()])
        query = f"UPDATE {table} SET {update_clauses} WHERE {filter_clauses};"

        try:
            self.adapter.execute(query, tuple(updates.values()) + tuple(filters.values()))
            if not getattr(self.local, 'in_transaction', False):
                self.adapter.commit()
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)

    def delete_records(self, table, filters):
        """
        Delete records from the specified table.

        Args:
            table (str): The name of the table.
            filters (Dict[str, Any]): The filters to identify the records to delete.
        """

        if not filters:
            raise ValidationError("No filters provided")

        self._validate_filters(table, filters)

        filter_clauses = ' AND '.join([f"{k} = ?" for k in filters.keys()])
        query = f"DELETE FROM {table} WHERE {filter_clauses};"
        try:
            self.adapter.execute(query, tuple(filters.values()))
            if not getattr(self.local, 'in_transaction', False):
                self.adapter.commit()
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)
