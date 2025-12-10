from dagster import Definitions, load_assets_from_modules
from ..assets.dbt import analytics
from ..resources import dbt_resource

dbt_analytics_assets = load_assets_from_modules(modules=[analytics])

defs = Definitions(
    assets=[*dbt_analytics_assets],
    resources={"dbt": dbt_resource},
)
