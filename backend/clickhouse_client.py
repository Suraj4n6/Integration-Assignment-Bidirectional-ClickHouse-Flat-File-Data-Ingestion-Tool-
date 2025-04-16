from clickhouse_connect import get_client
from models import ClickHouseConfig

def connect_clickhouse(config: ClickHouseConfig):
    try:
        client = get_client(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.jwt_token,
            database=config.database,
            secure=True
        )
        return client
    except Exception as e:
        raise Exception(f"ClickHouse connection failed: {e}")

def get_clickhouse_tables(client):
    try:
        return client.query("SHOW TABLES").result_rows
    except Exception as e:
        raise Exception(f"Failed to fetch tables: {e}")
def fetch_data(client, table_name: str, columns: list[str]):
    try:
        column_str = ", ".join(columns)
        query = f"SELECT {column_str} FROM {table_name}"
        result = client.query(query)
        return result.result_rows
    except Exception as e:
        raise Exception(f"Error fetching data: {e}")
