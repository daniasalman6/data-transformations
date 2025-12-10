import pytest
from unittest.mock import patch, MagicMock
from data_transformations.resources.clickhouse import ClickHouseResource


@pytest.fixture
def clickhouse_resource():
    """Fixture to initialize ClickHouseResource"""
    return ClickHouseResource(
        host="localhost",
        port=8123,
        database="test_db",
        username="test_user",
        password="test_pass",
    )


@patch("data_transformations.resources.clickhouse.ClickHouseResource.get_client")
def test_create_database(mock_get_client, clickhouse_resource):
    """Test the create_database method ensures database creation."""
    mock_client = MagicMock()
    mock_get_client.return_value.__enter__.return_value = mock_client
    mock_client.command = MagicMock()

    clickhouse_resource.create_database("test_db")

    mock_client.command.assert_called_once_with("CREATE DATABASE IF NOT EXISTS test_db;")


@patch("data_transformations.resources.clickhouse.ClickHouseResource.get_client")
def test_create_airbyte_table(mock_get_client, clickhouse_resource):
    """Test the create_airbyte_table method for table creation with TTL."""
    mock_client = MagicMock()
    mock_get_client.return_value.__enter__.return_value = mock_client  # Fix for `with` statement
    mock_client.command = MagicMock()

    clickhouse_resource.create_airbyte_table("test_db", "test_table")

    # Extract actual queries executed
    actual_queries = [call_args[0][0].strip() for call_args in mock_client.command.call_args_list]

    # Define expected queries (without unnecessary indentation)
    expected_queries = [
        "CREATE DATABASE IF NOT EXISTS test_db;",
        """CREATE TABLE IF NOT EXISTS test_db.test_table (
            `_airbyte_ab_id` String,
            `_airbyte_data` String,
            `_airbyte_emitted_at` DateTime64(3, 'GMT') DEFAULT now()
        ) ENGINE = MergeTree()
        PRIMARY KEY _airbyte_ab_id
        ORDER BY _airbyte_ab_id
        TTL toDateTime(_airbyte_emitted_at) + INTERVAL 1 WEEK;""",
    ]

    # Check the number of queries executed
    assert len(actual_queries) == len(expected_queries), (
        f"Expected {len(expected_queries)} calls, but got {len(actual_queries)}"
    )

    # Normalize whitespace, remove multiple spaces, and compare
    def normalize_query(query):
        return " ".join(query.split())

    for expected, actual in zip(expected_queries, actual_queries):
        assert normalize_query(expected) == normalize_query(actual), (
            f"Query mismatch:\nExpected:\n{expected}\nGot:\n{actual}"
        )


@patch("data_transformations.resources.clickhouse.ClickHouseResource.get_client")
def test_create_airbyte_table_handles_exceptions(mock_get_client, clickhouse_resource):
    """Test that create_airbyte_table raises an exception if table creation fails."""
    mock_client = MagicMock()
    mock_get_client.return_value.__enter__.return_value = mock_client
    mock_client.command.side_effect = Exception("ClickHouse error")  # Simulate an error

    with pytest.raises(Exception, match="ClickHouse error"):
        clickhouse_resource.create_airbyte_table("test_db", "test_table")
