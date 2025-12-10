from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from ...project import dbt_project
from .translator.dagster_dbt_translator import HistoricalDagsterDbtTranslator


@dbt_assets(
    manifest=dbt_project.manifest_path, dagster_dbt_translator=HistoricalDagsterDbtTranslator(), select="history"
)
def dbt_historical(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Executes a dbt build command to run historical models.

    This function is decorated as a Dagster dbt asset, using the specified dbt manifest
    and a custom Dagster-DBT translator. It includes the "history" models from execution.

    Args:
        context (AssetExecutionContext): The execution context provided by Dagster.
        dbt (DbtCliResource): The dbt CLI resource used to run dbt commands.

    Yields:
        DbtCliInvocation: A stream of events from the dbt CLI command execution.
    """
    yield from dbt.cli(["build"], context=context).stream()
