from dagster import Definitions, load_assets_from_modules

from ..assets.dbt import historical
from ..assets.airbyte import build_airbyte_asset, build_airbyte_table_asset
from ..configs.airbyte import load_airbyte_config
from ..resources import dbt_resource, airbyte_resource, clickhouse_resource

# Load Airbyte configurations
airbyte_configs = load_airbyte_config()

# Load assets from historical dbt module
dbt_assets = load_assets_from_modules(modules=[historical])

# Build assets dynamically from Airbyte configurations
airbyte_assets = [build_airbyte_asset(config) for config in airbyte_configs.connections]

# Build Airbyte table assets dynamically
airbyte_table_assets = [
    asset for config in airbyte_configs.connections for asset in build_airbyte_table_asset(config)
]  # Flatten the list

defs = Definitions(
    assets=[*airbyte_assets, *airbyte_table_assets, *dbt_assets],
    resources={
        "dbt": dbt_resource,
        "airbyte": airbyte_resource,
        "clickhouse": clickhouse_resource,
    },
)
