# Constants for Airbyte source setup
INVALID_CDC_POSITION_BEHAVIOUR = "Fail sync"
CDC_REPLICATION_METHOD = "CDC"
INITIAL_WAITING_SECONDS = 2400
INITIAL_LOAD_TIMEOUT_HOURS = 8
LSN_COMMIT_BEHAVIOUR = "After loading Data in the destination"
SOURCE_DEFINITION_ID = "decd338e-5647-4c0b-adf4-da0e75f5a750"

# Constants for Airbyte destination setup
DESTINATION_CONNECTOR_VERSION = "0.2.5"
DESTINATION_DEFINITION_ID = "ce0d828e-1dc4-496c-b122-2da42e637e48"

# Constants for Airbyte connection setup
CRON_TIME_ZONE = "UTC"
SCHEDULE_TYPE = "cron"
STATUS_ACTIVE = "active"
SYNC_CATALOG = {"streams": []}
NAMESPACE_DEFINITION = "customformat"
OPERATION_IDS = []
SCHEMA_CHANGE_PREFERENCE = "propagate_columns"

# Common constants
SSL_MODE = {"mode": "disable"}
TUNNEL_METHOD = {"tunnel_method": "NO_TUNNEL"}
