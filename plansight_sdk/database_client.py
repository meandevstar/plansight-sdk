import re
from .exceptions import DatabaseError, RecordNotFoundError, ValidationError, ConstraintViolationError
from .schema.schema_config import SCHEMA_CONFIG
from .adapters.sqlite_adapter import SQLiteAdapter

class DatabaseClient:
    def __init__(self, db_config, db_type='sqlite'):
        self.db_config = db_config
        self.db_type = db_type
        self.adapter = self._get_adapter()

    def _get_adapter(self):
        if self.db_type == 'sqlite':
            return SQLiteAdapter()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def __enter__(self):
        print('==> with enter called')
        self.adapter.connect(self.db_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('==> with exit called', exc_type, exc_val, exc_tb)
        if exc_type:
            self.adapter.rollback()
        else:
            self.adapter.commit()
        self.adapter.close()

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

    def create_record(self, table, data):
        self._validate_data(table, data)

        keys = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({keys}) VALUES ({placeholders});"

        try:
            self.adapter.execute(query, tuple(data.values()))
            self.adapter.commit()
            return self.adapter.cursor.lastrowid
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)

    def read_records(self, table, filters=None, order_by=None, limit=None):
        if filters:
            self._validate_filters(table, filters)

        query = f"SELECT * FROM {table}"
        if filters:
            filter_clauses = ' AND '.join([f"{k} = ?" for k in filters.keys()])
            query += f" WHERE {filter_clauses}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        try:
            print('==> query', f'{query};')
            self.adapter.execute(f'{query};', tuple(filters.values()) if filters else ())
            return self.adapter.fetchall()
        except DatabaseError as e:
            raise DatabaseError(e)

    def update_records(self, table, updates, filters):
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
            self.adapter.commit()
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)

    def delete_records(self, table, filters):
        if not filters:
            raise ValidationError("No filters provided")

        self._validate_filters(table, filters)

        filter_clauses = ' AND '.join([f"{k} = ?" for k in filters.keys()])
        query = f"DELETE FROM {table} WHERE {filter_clauses};"
        try:
            self.adapter.execute(query, tuple(filters.values()))
            self.adapter.commit()
        except ConstraintViolationError as e:
            raise ConstraintViolationError(e)
        except DatabaseError as e:
            raise DatabaseError(e)
