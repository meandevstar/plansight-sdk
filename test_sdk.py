import os
from plansight_sdk import DatabaseClient

def main():
    db_config = ':memory:'
    db_client = DatabaseClient(db_config, 'sqlite')

    with db_client as client:
        client.setup_tables()
    
        schema_path = os.path.join(os.path.dirname(__file__), 'data', 'seed.sql')
        with open(schema_path, 'r') as file:
            seed_sql = file.read()
        client.execute_statements(seed_sql)

        data = {
            'name': 'John Doe',
            'email': 'john.doe@example.com',
            'created_at': '2023-05-01 12:00:00'
        }
        record_id = client.create_record('Users', data)
        print(f"Record created with ID: {record_id}")

        filters = {'email': 'john.doe@example.com'}
        records = client.read_records('Users', filters=filters)
        print(records)

if __name__ == "__main__":
    main()