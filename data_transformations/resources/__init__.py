from dagster_dbt import DbtCliResource
from ..project import dbt_project
from .airbyte import AirbyteWorkspaceConfigs
from .clickhouse import ClickHouseResource
from ..configs import configs


dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

airbyte_resource = AirbyteWorkspaceConfigs()

clickhouse_resource = ClickHouseResource(
    host=configs.CLICKHOUSE_HOST,
    port=configs.CLICKHOUSE_PORT,
    database=configs.CLICKHOUSE_DATABASE,
    username=configs.CLICKHOUSE_DBT_USERNAME,
    password=configs.CLICKHOUSE_DBT_PASSWORD,
)
