from typing import Any, Mapping, Optional
from dagster_dbt import DagsterDbtTranslator
from dagster import AutomationCondition, AssetKey
from ....utils.automation import (
    get_default_automation_condition,
)


class BaseDagsterDbtTranslator(DagsterDbtTranslator):
    """
    A base class for translating dbt resource properties into Dagster-specific attributes.
    Provides default implementations for asset grouping and asset keys.
    """

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        """
        Derives the group name from the fully qualified name (FQN) of the dbt resource.

        Args:
            dbt_resource_props (Mapping[str, Any]): The dbt resource properties.

        Returns:
            str: The generated group name <layer>_<database> e.g. raw_customer, stg_ledger etc
        """
        return f"{dbt_resource_props['fqn'][1]}_{dbt_resource_props['fqn'][2]}"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """
        Generates an AssetKey using the name of the dbt resource.

        Args:
            dbt_resource_props (Mapping[str, Any]): The dbt resource properties.

        Returns:
            AssetKey: The corresponding Dagster AssetKey.
        """
        # AssetKey will be the name of dbt model e.g. raw_customer__customers
        return AssetKey(dbt_resource_props["name"])

    def get_automation_condition(self, dbt_resource_props: Mapping[str, Any]) -> Optional[AutomationCondition]:
        """
        Defines an auto-materialization condition for dbt assets.

        Args:
            dbt_resource_props (Mapping[str, Any]): The dbt resource properties.

        Returns:
            Optional[AutomationCondition]: The auto-materialization condition.
        """
        condition = get_default_automation_condition()

        return condition


class AnalyticsDagsterDbtTranslator(BaseDagsterDbtTranslator):
    """
    A Dagster-DBT translator specifically for Analytics pipelines.
    It assigns tags and defines an auto-materialization policy.
    """

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        """
        Assigns tags for the analytics pipeline based on the dbt resource properties.

        Args:
            dbt_resource_props (Mapping[str, Any]): The dbt resource properties.

        Returns:
            Mapping[str, str]: A dictionary of tags including pipeline and layer.
        """
        return {"pipeline": "analytics", "layer": dbt_resource_props["fqn"][1]}


class HistoricalDagsterDbtTranslator(BaseDagsterDbtTranslator):
    """
    A Dagster-DBT translator for historical data pipelines.
    It assigns appropriate tags to dbt resources.
    """

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
        """
        Assigns tags for the historical pipeline based on the dbt resource properties.

        Args:
            dbt_resource_props (Mapping[str, Any]): The dbt resource properties.

        Returns:
            Mapping[str, str]: A dictionary of tags including pipeline and layer.
        """
        return {"pipeline": "history", "layer": dbt_resource_props["fqn"][1]}
