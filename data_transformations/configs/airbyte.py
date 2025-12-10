import os
import requests
import requests.auth
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
import yaml
from pathlib import Path
from ..configs import configs as conf
from .constants import *
from dagster import get_dagster_logger

logger = get_dagster_logger()

AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME", "")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD", "")
AIRBYTE_API_URL = os.getenv("AIRBYTE_API_URL", "")

AIRBYTE_CONFIG_PATH = Path(os.getenv("AIRBYTE_CONFIG_PATH", ""))

# api requests configs
API_REQUEST_HEADER = {
    "Content-Type": "application/json",
}
API_AUTH = requests.auth.HTTPBasicAuth(AIRBYTE_USERNAME, AIRBYTE_PASSWORD)

CLICKHOUSE_AIRBYTE_USERNAME = os.getenv("CLICKHOUSE_AIRBYTE_USERNAME", "")
CLICKHOUSE_AIRBYTE_PASSWORD = os.getenv("CLICKHOUSE_AIRBYTE_PASSWORD", "")


# PYDANTIC MODELS TO INITIALIZE SOURCE, DESTINATION AND CONNECTION SETUPS THROUGH CONFIG MAPS
class SourceConnectionReplicationMethod(BaseModel):
    replication_slot: str
    publication: str
    heartbeat_action_query: Optional[str] = None
    queue_size: int = 1000
    invalid_cdc_position_behaviour: str = INVALID_CDC_POSITION_BEHAVIOUR
    method: str = CDC_REPLICATION_METHOD
    initial_waiting_seconds: int = INITIAL_WAITING_SECONDS
    initial_load_timeout_hours: int = INITIAL_LOAD_TIMEOUT_HOURS
    lsn_commit_behaviour: str = LSN_COMMIT_BEHAVIOUR


class SourceConnectionConfiguration(BaseModel):
    host: str = conf.POSTGRES_HOST
    port: int = conf.POSTGRES_PORT
    database: str
    schemas: List[str]
    username: str
    password: str
    ssl_mode: Dict[str, str] = SSL_MODE
    tunnel_method: Dict[str, str] = TUNNEL_METHOD
    replication_method: SourceConnectionReplicationMethod


class AirbyteSourceConfig(BaseModel):
    name: str
    connectionConfiguration: SourceConnectionConfiguration
    workspaceId: Optional[str] = None
    sourceDefinitionId: str = SOURCE_DEFINITION_ID


class DestinationConnectionConfiguration(BaseModel):
    host: str = conf.CLICKHOUSE_HOST
    port: int = conf.CLICKHOUSE_PORT
    database: str = conf.CLICKHOUSE_DATABASE
    username: str = CLICKHOUSE_AIRBYTE_USERNAME
    password: str = CLICKHOUSE_AIRBYTE_PASSWORD
    ssl_mode: Dict[str, str] = SSL_MODE
    tunnel_method: Dict[str, str] = TUNNEL_METHOD
    version: str = DESTINATION_CONNECTOR_VERSION


class AirbyteDestinationConfig(BaseModel):
    workspaceId: Optional[str] = None
    name: str
    destinationDefinitionId: str = DESTINATION_DEFINITION_ID
    connectionConfiguration: DestinationConnectionConfiguration = Field(
        default_factory=DestinationConnectionConfiguration
    )


class CronModel(BaseModel):
    cronExpression: str
    cronTimeZone: str = CRON_TIME_ZONE


class ScheduleDataModel(BaseModel):
    cron: CronModel


class AirbyteConnectionConfig(BaseModel):
    scheduleType: str = SCHEDULE_TYPE
    scheduleData: ScheduleDataModel
    status: str = STATUS_ACTIVE
    name: str
    workspaceId: Optional[str] = None
    sourceId: Optional[str] = None
    destinationId: Optional[str] = None
    syncCatalog: dict[str, list] = SYNC_CATALOG
    namespaceDefinition: str = NAMESPACE_DEFINITION
    namespaceFormat: str
    prefix: str = ""
    operationIds: list = OPERATION_IDS
    nonBreakingChangesPreference: str = SCHEMA_CHANGE_PREFERENCE


class AirbyteSyncConfig(BaseModel):
    name: str
    source: AirbyteSourceConfig
    destination: AirbyteDestinationConfig
    connection: AirbyteConnectionConfig
    streams: list
    asset_name: str
    # TTL interval in clickhouse defines the time period after which rows will be deleted automatically.
    # Reference: https://clickhouse.com/docs/guides/developer/ttl#ttl-syntax
    ttl_interval: str


class AirbyteConfig(BaseModel):
    connections: List[AirbyteSyncConfig]


# LOADS YAML CONTENT FROM CONFIG MAPS INTO PYDANTIC MODEL
def load_airbyte_config() -> AirbyteConfig:
    config_data = {"connections": []}
    if AIRBYTE_CONFIG_PATH.is_file():
        content = AIRBYTE_CONFIG_PATH.read_text()

        # expanding on the environment secrets
        content = os.path.expandvars(content)
        config_data = yaml.safe_load(content)
    else:
        logger.warning(f"Did not find a config file at path: {AIRBYTE_CONFIG_PATH}")

    return AirbyteConfig(**config_data)
