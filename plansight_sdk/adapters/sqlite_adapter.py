import sqlite3
from .base_adapter import BaseAdapter
from ..exceptions import DatabaseError, ConstraintViolationError

class SQLiteAdapter(BaseAdapter):
    def connect(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None

    def execute(self, query, params=None):
        if params is None:
            params = ()
        try:
            self.cursor.execute(query, params)
        except sqlite3.IntegrityError as e:
            raise ConstraintViolationError(e)
        except sqlite3.Error as e:
            raise DatabaseError(e)

    def fetchall(self):
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
