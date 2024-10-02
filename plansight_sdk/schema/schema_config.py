SCHEMA_CONFIG = {
    'Users': {
        'required_fields': ['name', 'email', 'created_at'],
        'field_types': {
            'user_id': int,
            'name': str,
            'email': str,
            'created_at': str
        },
        'field_constraints': {
            'name': 100,
            'email': 100
        }
    },
    'Activities': {
        'required_fields': ['user_id', 'activity_type', 'timestamp'],
        'field_types': {
            'activity_id': int,
            'user_id': int,
            'activity_type': str,
            'timestamp': str,
            'metadata': str
        },
        'field_constraints': {
            'activity_type': 50,
            'metadata': 255
        }
    }
}
