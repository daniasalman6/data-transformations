import pytest
from data_transformations.assets.dbt.translator.dagster_dbt_translator import (
    BaseDagsterDbtTranslator,
    AnalyticsDagsterDbtTranslator,
    HistoricalDagsterDbtTranslator,
)
from dagster import AssetKey


@pytest.fixture
def dbt_resource_props():
    return {"fqn": ["project", "stg", "ledger", "transactions"], "name": "stg_ledger__transactions"}


@pytest.fixture
def base_translator():
    return BaseDagsterDbtTranslator()


@pytest.fixture
def analytics_translator():
    return AnalyticsDagsterDbtTranslator()


@pytest.fixture
def historical_translator():
    return HistoricalDagsterDbtTranslator()


def test_get_group_name(base_translator, dbt_resource_props):
    assert base_translator.get_group_name(dbt_resource_props) == "stg_ledger"


def test_get_asset_key(base_translator, dbt_resource_props):
    assert base_translator.get_asset_key(dbt_resource_props) == AssetKey("stg_ledger__transactions")


def test_analytics_get_tags(analytics_translator, dbt_resource_props):
    assert analytics_translator.get_tags(dbt_resource_props) == {"pipeline": "analytics", "layer": "stg"}


def test_historical_get_tags(historical_translator, dbt_resource_props):
    assert historical_translator.get_tags(dbt_resource_props) == {"pipeline": "history", "layer": "stg"}
