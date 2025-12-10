from dagster import asset, AssetsDefinition, AutomationCondition, AssetKey, AssetExecutionContext, Failure
from ..utils.airbyte import (
    create_airbyte_resource,
    get_resource_id,
    discover_schema,
    add_sync_configurations,
)
from ..configs.airbyte import AirbyteSyncConfig
from ..resources.airbyte import AirbyteWorkspaceConfigs
from ..resources.clickhouse import ClickHouseResource
from ..utils.automation import (
    get_default_automation_condition,
)


def build_airbyte_asset(airbyte_sync_config: AirbyteSyncConfig) -> AssetsDefinition:
    """
    Constructs and returns a Dagster asset for managing Airbyte sync configurations.

    This function defines an asset that:
    1. Retrieves the workspace ID from the Airbyte workspace configurations.
    2. Sets up the source, destination, and connection entities in Airbyte.
    3. Fetches the schema for specified streams from the source.
    4. Configures the connection with the discovered schema and additional sync settings.

    Args:
        airbyte_sync_config (AirbyteSyncConfig):
            A configuration object containing source, destination, connection, streams and
            asset name details.

    Returns:
        AssetsDefinition:
            A Dagster asset responsible for orchestrating the Airbyte sync setup.

    Raises:
        RuntimeError: If any API call fails while setting up entities or fetching schema.
    """
    required_clickhouse_assets = {AssetKey(f"_airbyte_raw_{stream}") for stream in airbyte_sync_config.streams}

    @asset(
        name=airbyte_sync_config.asset_name,
        kinds={"airbyte"},
        group_name="airbyte",
        deps=required_clickhouse_assets,
        automation_condition=get_default_automation_condition(),
    )
    def _asset(airbyte: AirbyteWorkspaceConfigs):
        # Fetch workspace ID and add it to source, destination and connection models
        workspace_id = airbyte.workspace_id()
        airbyte_sync_config.source.workspaceId = workspace_id
        airbyte_sync_config.destination.workspaceId = workspace_id
        airbyte_sync_config.connection.workspaceId = workspace_id

        ### SOURCE SETUP
        # fetching source name from source model
        source_name = airbyte_sync_config.source.name

        # converts the pydantic model to json
        payload = airbyte_sync_config.source.model_dump()
        create_airbyte_resource(
            resource_type="source",
            workspace_id=workspace_id,
            resource_name=source_name,
            payload=payload,
        )

        ### DESTINATION SETUP
        destination_name = airbyte_sync_config.destination.name
        payload = airbyte_sync_config.destination.model_dump()
        create_airbyte_resource(
            resource_type="destination",
            workspace_id=workspace_id,
            resource_name=destination_name,
            payload=payload,
        )

        ### CONNECTION SETUP
        # Setting source and destination ids that we have set up above in connection model
        source_id = get_resource_id(resource_type="source", resource_name=source_name, workspace_id=workspace_id)
        destination_id = get_resource_id(
            resource_type="destination",
            resource_name=destination_name,
            workspace_id=workspace_id,
        )
        airbyte_sync_config.connection.sourceId = source_id
        airbyte_sync_config.connection.destinationId = destination_id

        # Fetching specified streams from the source schema
        streams = airbyte_sync_config.streams

        # Filtered catalog with specified streams
        sync_catalog = discover_schema(source_id, streams=streams)

        # Adding custom configurations to the filtered streams
        configured_sync_catalog = add_sync_configurations(sync_catalog=sync_catalog)
        airbyte_sync_config.connection.syncCatalog = {"streams": configured_sync_catalog}
        connection_name = airbyte_sync_config.connection.name
        payload = airbyte_sync_config.connection.model_dump()
        create_airbyte_resource(
            resource_type="connection",
            workspace_id=workspace_id,
            resource_name=connection_name,
            payload=payload,
        )

    return _asset


def build_airbyte_table_asset(airbyte_sync_config: AirbyteSyncConfig) -> list[AssetsDefinition]:
    """
    Builds a list of Dagster assets for creating Airbyte raw tables in ClickHouse.

    - Each stream gets its own `_airbyte_raw_<stream_name>` table.

    Args:
        airbyte_sync_config (AirbyteSyncConfig):
            A configuration object containing source, destination, connection, streams and
            asset name details.

    Returns:
        AssetsDefinition:
            A Dagster asset responsible for orchestrating the clickhouse airbyte tables creation.

    Raises:
        Dagster Job Failure if Clickhouse table creation fails.
    """

    assets = []

    for stream in airbyte_sync_config.streams:

        def create_asset_function(stream_name):  # Function factory to capture each stream separately
            @asset(
                name=f"_airbyte_raw_{stream_name}",
                group_name="airbyte_tables",
                kinds={"clickhouse"},
                automation_condition=AutomationCondition.missing().with_label("materialize_if_missing"),
            )
            def _asset(context: AssetExecutionContext, clickhouse: ClickHouseResource):
                database = airbyte_sync_config.destination.name
                table_name = f"_airbyte_raw_{stream_name}"
                ttl_interval = airbyte_sync_config.ttl_interval

                try:
                    context.log.info(f"Attempting to create table: {database}.{table_name}")
                    clickhouse.create_airbyte_table(database=database, table_name=table_name, ttl_interval=ttl_interval)
                    context.log.info(f"Successfully created table: {database}.{table_name}")

                except Exception as e:
                    error_message = f"Failed to create ClickHouse table '{database}.{table_name}': {e}"
                    context.log.error(error_message)
                    raise Failure(description=error_message)

            return _asset  # Return a separate function

        assets.append(create_asset_function(stream))  # Create and append separate asset

    return assets
