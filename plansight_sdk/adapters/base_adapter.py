class BaseAdapter:
    def connect(self, db_path):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def execute(self, query, params=None):
        raise NotImplementedError

    def fetchall(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError
