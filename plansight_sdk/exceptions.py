class DatabaseError(Exception):
    pass

class RecordNotFoundError(DatabaseError):
    pass

class ValidationError(DatabaseError):
    pass

class ConstraintViolationError(DatabaseError):
    pass
