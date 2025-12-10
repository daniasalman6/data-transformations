from unittest import mock
import importlib
import pathlib


def test_load_airbyte_config():
    # Mock environment variables
    with mock.patch.dict(
        "os.environ",
        {
            "TEST_RO_USERNAME": "mock_user",
            "TEST_RO_PASSWORD": "mock_password",
            "AIRBYTE_CONFIG_PATH": "/mock/path/to/config.yaml",
        },
    ):
        mock_yaml_content = """
            connections:
                - name: "test"
                  source:
                    name: "test"
                    connectionConfiguration:
                      database: "test"
                      schemas:
                        - "test"
                      username: $TEST_RO_USERNAME
                      password: $TEST_RO_PASSWORD
                      replication_method:
                        replication_slot: "airbyte_test_slot"
                        publication: "airbyte_publication"
                        heartbeat_action_query: "UPDATE test.heartbeats SET updated = CURRENT_TIMESTAMP WHERE id = 1;"
                  destination:
                    name: "hist_test"
                  connection:
                    scheduleData:
                      cron:
                        cronExpression: "0 0 0 * * ? *"
                    name: "test -> hist_test"
                    namespaceFormat: "hist_test"
                  streams:
                    - "stream1"
                    - "stream2"
                  asset_name: "airbyte_test_connection"
                  ttl_interval: "1 WEEK"
            """
        # Patch Path object before importing the module
        with (
            mock.patch.object(pathlib.Path, "is_file", return_value=True),
            mock.patch.object(pathlib.Path, "read_text", return_value=mock_yaml_content),
        ):
            # Import the module after patching
            import data_transformations.configs.configs

            # Reload to ensure patch is used during initialization
            importlib.reload(data_transformations.configs.configs)

            from data_transformations.configs.airbyte import (
                load_airbyte_config,
                AirbyteConfig,
            )

            config = load_airbyte_config()
            assert isinstance(config, AirbyteConfig)
            assert len(config.connections) == 1

            connection = config.connections[0]
            assert connection.source.name == "test"
            assert connection.source.connectionConfiguration.username == "mock_user"
            assert connection.source.connectionConfiguration.password == "mock_password"
            assert connection.destination.name == "hist_test"
            assert connection.connection.name == "test -> hist_test"
            assert connection.connection.namespaceFormat == "hist_test"
            assert connection.streams == ["stream1", "stream2"]
            assert connection.asset_name == "airbyte_test_connection"
            assert connection.ttl_interval == "1 WEEK"
