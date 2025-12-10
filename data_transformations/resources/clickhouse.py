from dagster import ConfigurableResource, get_dagster_logger
from clickhouse_connect import get_client

logger = get_dagster_logger()


class ClickHouseResource(ConfigurableResource):
    """
    A Dagster resource for interacting with a ClickHouse database.
    Attributes:
        host (str): The ClickHouse server hostname.
        port (int): The ClickHouse server port.
        database (str): The target database name.
        username (str): The username for authentication.
        password (str): The password for authentication.
    """

    host: str
    port: int
    database: str
    username: str
    password: str

    def get_client(self):
        """
        Creates and returns a ClickHouse client instance.
        Returns:
            clickhouse_connect.client.Client: A client instance for interacting with ClickHouse.
        """
        return get_client(host=self.host, port=self.port, username=self.username, password=self.password)

    def create_database(self, database: str):
        """
        Ensures the database exists in ClickHouse.
        Args:
            database (str): The database name.
        """
        query = f"CREATE DATABASE IF NOT EXISTS {database};"
        try:
            with self.get_client() as client:
                client.command(query)
                logger.info(f"Database '{database}' ensured.")
        except Exception as e:
            logger.error(f"Failed to create database '{database}': {e}")

    def create_airbyte_table(self, database: str, table_name: str, ttl_interval: str = "1 WEEK"):
        """
        Checks whether a given airbyte table exists or not and then creates table accordingly in the specified ClickHouse database.
        Args:
            database (str): The database name.
            table_name (str): The table name to create.
        Exception:
                 If the table creation fails.
        """
        self.create_database(database)

        query = f"""
                CREATE TABLE IF NOT EXISTS {database}.{table_name} (
                    `_airbyte_ab_id` String,
                    `_airbyte_data` String,
                    `_airbyte_emitted_at` DateTime64(3, 'GMT') DEFAULT now()
                ) ENGINE = MergeTree()
                PRIMARY KEY _airbyte_ab_id
                ORDER BY _airbyte_ab_id
                TTL toDateTime(_airbyte_emitted_at) + INTERVAL {ttl_interval};
                """
        try:
            with self.get_client() as client:
                client.command(query)
                logger.info(f"Table '{database}.{table_name}' created successfully with TTL.")
        except Exception as e:
            logger.error(f"Failed to create table '{database}.{table_name}': {e}")
            raise e
