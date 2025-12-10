import pytest
from unittest import mock
import requests
from data_transformations.utils.airbyte import (
    api_post_request,
    get_workspace_id,
    create_airbyte_resource,
    discover_schema,
    get_resource_id,
    add_sync_configurations,
)


### PYTEST FIXTURE USED FOR MOCK POST REQUEST
@pytest.fixture
def mock_post_request():
    with mock.patch("requests.post") as mock_post:
        yield mock_post


### PYTEST FIXTURE TO MOCK THE API_POST_REQUEST FUNCTION TO AVOID REAL API CALLS
@pytest.fixture
def mock_api_post_request():
    with mock.patch("data_transformations.utils.airbyte.api_post_request") as mock_request:
        yield mock_request


### PYTEST FIXTURE TO MOCK THE GET_RESOURCE_ID FUNCTION TO CONTROL ITS RETURN VALUE
@pytest.fixture
def mock_get_resource_id():
    with mock.patch("data_transformations.utils.airbyte.get_resource_id") as mock_resource_id:
        yield mock_resource_id


###
# PYTESTS FOR api_post_request() FUNCTION
###
def test_api_post_request_success(mock_post_request):
    api_endpoint = "/test-endpoint"
    payload = {"key": "value"}
    expected_response = {"success": True}

    # Mocking the response for a successful request
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = expected_response
    mock_post_request.return_value = mock_response

    result = api_post_request(api_endpoint, payload)
    assert result == expected_response


def test_api_post_request_error_status(mock_post_request):
    api_endpoint = "/test-endpoint"
    payload = {"key": "value"}

    # Mocking the response with an error status code
    mock_response = mock.Mock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_response.raise_for_status.side_effect = requests.HTTPError("Bad Request")
    mock_post_request.return_value = mock_response

    with pytest.raises(RuntimeError, match="An unexpected error ocurred: Bad Request"):
        api_post_request(api_endpoint, payload)


def test_api_post_request_request_exception(mock_post_request):
    api_endpoint = "/test-endpoint"
    payload = {"key": "value"}

    # Mocking a network-related exception
    mock_post_request.side_effect = requests.RequestException("Network error")
    with pytest.raises(RuntimeError, match="An unexpected error ocurred: Network error"):
        api_post_request(api_endpoint, payload)


###
# PYTESTS FOR get_workspace_id() FUNCTION
###
def test_get_workspace_id_success(mock_api_post_request):
    mock_api_post_request.return_value = {
        "workspaces": [
            {
                "workspaceId": "2bfdf324-c9de-4b68-b412-cf6e5b662456",
                "customerId": "3d9a2c6b-3b72-4978-8ed0-1a9b9a3e1a9e",
                "email": "testmail@test.com",
                "name": "Test Workspace",
                "slug": "e1f42c6e-3451-4ef6-bcd1-0f8a4f4f5c8a",
                "initialSetupComplete": True,
                "displaySetupWizard": False,
                "anonymousDataCollection": False,
                "notifications": [],
                "notificationSettings": {},
                "defaultGeography": "auto",
                "webhookConfigs": [],
                "organizationId": "a8b2acb5-f075-4c9a-a504-f73a721d4629",
                "tombstone": False,
            }
        ]
    }

    result = get_workspace_id()

    assert result == "2bfdf324-c9de-4b68-b412-cf6e5b662456"
    assert len(result) == 36


def test_get_workspace_id_no_workspaces(mock_api_post_request):
    mock_api_post_request.return_value = {"workspaces": []}

    with pytest.raises(RuntimeError, match="No workspaces found in the response."):
        get_workspace_id()


###
# PYTESTS FOR get_workspace_id() FUNCTION
###
def test_airbyte_resource_setup_existing_resource(mock_api_post_request, mock_get_resource_id):
    # Test scenario where resource already exists
    mock_get_resource_id.return_value = "existing-resource-id"

    create_airbyte_resource(
        resource_type="source",
        workspace_id="workspace-123",
        resource_name="existing-source",
        payload={},
    )

    # Assert that api_post_request was never called because resource exists
    mock_api_post_request.assert_not_called()


def test_airbyte_resource_setup_create_new_resource(mock_api_post_request, mock_get_resource_id):
    # Test scenario where resource doesn't exist and a new one is created
    mock_get_resource_id.return_value = None
    mock_api_post_request.return_value = {"success": True}

    create_airbyte_resource(
        resource_type="destination",
        workspace_id="workspace-123",
        resource_name="new-destination",
        payload={"config": "value"},
    )

    # Assert that api_post_request was called once with correct arguments
    mock_api_post_request.assert_called_once_with("/destinations/create", payload={"config": "value"})


def test_airbyte_resource_setup_invalid_resource_type(mock_api_post_request):
    # Test scenario with an invalid resource type
    create_airbyte_resource(
        resource_type="invalid",
        workspace_id="workspace-123",
        resource_name="invalid-resource",
        payload={},
    )

    # Assert that api_post_request was never called due to invalid resource type
    mock_api_post_request.assert_not_called()


def test_airbyte_resource_setup_failed_creation(mock_api_post_request, mock_get_resource_id):
    # Test scenario where API request fails to create the resource
    mock_get_resource_id.return_value = None
    mock_api_post_request.return_value = None  # Simulate API failure

    with pytest.raises(RuntimeError, match="Failed to set up source 'new-source'."):
        create_airbyte_resource(
            resource_type="source",
            workspace_id="workspace-123",
            resource_name="new-source",
            payload={"config": "value"},
        )


###
# PYTESTS FOR discover_schema() FUNCTION
###

# Common mock response for multiple tests
mock_catalog_response = {
    "catalog": {
        "streams": [
            {
                "stream": {
                    "name": "stream1",
                    "jsonSchema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "string"},
                            "col2": {"type": "string"},
                            "_ab_cdc_lsn": {"type": "number"},
                        },
                    },
                    "supportedSyncModes": ["full_refresh", "incremental"],
                    "sourceDefinedCursor": True,
                    "defaultCursorField": ["_ab_cdc_lsn"],
                    "sourceDefinedPrimaryKey": [["col1"]],
                    "namespace": "database",
                    "isResumable": True,
                },
                "config": {
                    "syncMode": "incremental",
                    "cursorField": ["_ab_cdc_lsn"],
                    "destinationSyncMode": "append_dedup",
                    "primaryKey": [["col1"]],
                    "aliasName": "stream1",
                    "selected": False,
                    "suggested": False,
                    "selectedFields": [],
                    "hashedFields": [],
                    "mappers": [],
                },
            },
            {
                "stream": {
                    "name": "stream2",
                    "jsonSchema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "string"},
                            "_ab_cdc_lsn": {"type": "number"},
                        },
                    },
                    "supportedSyncModes": ["full_refresh", "incremental"],
                    "sourceDefinedCursor": True,
                    "defaultCursorField": ["_ab_cdc_lsn"],
                    "sourceDefinedPrimaryKey": [["col1"]],
                    "namespace": "database",
                    "isResumable": True,
                },
                "config": {
                    "syncMode": "incremental",
                    "cursorField": ["_ab_cdc_lsn"],
                    "destinationSyncMode": "append_dedup",
                    "primaryKey": [["col1"]],
                    "aliasName": "stream2",
                    "selected": False,
                    "suggested": False,
                    "selectedFields": [],
                    "hashedFields": [],
                    "mappers": [],
                },
            },
            {
                "stream": {
                    "name": "stream3",
                    "jsonSchema": {
                        "type": "object",
                        "properties": {
                            "col1": {"type": "string"},
                        },
                    },
                    "supportedSyncModes": ["full_refresh"],
                    "namespace": "database",
                },
                "config": {
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                    "aliasName": "stream3",
                },
            },
        ]
    }
}


def test_discover_schema_success(mock_api_post_request):
    source_id = "source-123"
    streams = ["stream1", "stream2"]
    mock_api_post_request.return_value = mock_catalog_response

    result = discover_schema(source_id, streams)

    expected_result = [
        mock_catalog_response["catalog"]["streams"][0],
        mock_catalog_response["catalog"]["streams"][1],
    ]
    assert result == expected_result


def test_discover_schema_no_catalog(mock_api_post_request):
    source_id = "source-123"
    streams = ["stream1"]

    # Mocking the response with no catalog
    mock_api_post_request.return_value = {}

    with pytest.raises(RuntimeError, match="Failed to retrieve catalog from source discovery response."):
        discover_schema(source_id, streams)


def test_discover_schema_streams_not_found(mock_api_post_request):
    source_id = "source-123"
    streams = ["non_existent_stream"]

    # Using the shared mock response to simulate no matching streams
    mock_api_post_request.return_value = mock_catalog_response

    # Assert RuntimeError is raised for streams not found
    with pytest.raises(RuntimeError, match="Error: Streams not found in source catalog."):
        discover_schema(source_id, streams)


###
# PYTESTS FOR get_resource_id() FUNCTION
###

# common api response for multiple tests
resources_response = {
    "sources": [
        {
            "sourceDefinitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
            "sourceId": "2001c1b9-3124-4126-8d93-9023cf6e4ffe",
            "workspaceId": "e52a8057-ec93-4983-765a-acf28bbd0098",
            "connectionConfiguration": {
                "host": "test-host",
                "port": 1234,
                "schemas": ["schema1"],
                "database": "db1",
                "password": "password1",
                "ssl_mode": {"mode": "disable"},
                "username": "db1_user",
                "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                "replication_method": {
                    "method": "CDC",
                    "queue_size": 1000,
                    "publication": "db1_publication",
                    "replication_slot": "db1_slot",
                    "lsn_commit_behaviour": "After loading Data in the destination",
                    "heartbeat_action_query": "sample query;",
                    "initial_waiting_seconds": 120,
                    "initial_load_timeout_hours": 2,
                    "invalid_cdc_position_behaviour": "Fail sync",
                },
            },
            "name": "db1",
            "sourceName": "test-source",
            "icon": "https://connectors.airbyte.com/files/metadata/airbyte/source-test/latest/icon.svg",
            "isVersionOverrideApplied": False,
            "supportState": "supported",
            "status": "active",
            "createdAt": 1739303683,
        },
        {
            "sourceDefinitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
            "sourceId": "ae99f581-e988-44a1-9bab-7a9f4fbcd84d",
            "workspaceId": "e52a8057-ec93-4983-765a-acf28bbd0098",
            "connectionConfiguration": {
                "host": "test-host",
                "port": 1234,
                "schemas": ["schema2"],
                "database": "db2",
                "password": "password2",
                "ssl_mode": {"mode": "disable"},
                "username": "db2_user",
                "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                "replication_method": {
                    "method": "CDC",
                    "queue_size": 1000,
                    "publication": "db2_publication",
                    "replication_slot": "db2_slot",
                    "lsn_commit_behaviour": "After loading Data in the destination",
                    "heartbeat_action_query": "sample query;",
                    "initial_waiting_seconds": 120,
                    "initial_load_timeout_hours": 2,
                    "invalid_cdc_position_behaviour": "Fail sync",
                },
            },
            "name": "db2",
            "sourceName": "test-source",
            "icon": "https://connectors.airbyte.com/files/metadata/airbyte/source-test/latest/icon.svg",
            "isVersionOverrideApplied": False,
            "supportState": "supported",
            "status": "inactive",
            "createdAt": 1739428574,
        },
    ]
}


def test_get_resource_id_success(mock_api_post_request):
    # Test scenario where the resource is successfully found
    resource_type = "source"
    resource_name = "db2"
    workspace_id = "e52a8057-ec93-4983-765a-acf28bbd0098"
    mock_api_post_request.return_value = resources_response

    result = get_resource_id(resource_type, resource_name, workspace_id)

    assert result == "ae99f581-e988-44a1-9bab-7a9f4fbcd84d"
    assert len(result) == 36


def test_get_resource_id_not_found(mock_api_post_request):
    # Test scenario where the resource is not found in the list
    resource_type = "source"
    resource_name = "non-existent-source"
    workspace_id = "e52a8057-ec93-4983-765a-acf28bbd0098"
    mock_api_post_request.return_value = resources_response

    result = get_resource_id(resource_type, resource_name, workspace_id)

    assert result is None


def test_get_resource_id_invalid_resource_type():
    # Test scenario where an invalid resource type is provided
    with pytest.raises(
        ValueError,
        match="Invalid resource_type 'invalid_type'. Must be 'source', 'destination' or 'connection'.",
    ):
        get_resource_id("invalid_type", "resource-name", "e52a8057-ec93-4983-765a-acf28bbd0098")


def test_get_resource_id_no_resources(mock_api_post_request):
    # Test scenario where the API response is empty or doesn't contain the resource list
    resource_type = "source"
    resource_name = "db1"
    workspace_id = "e52a8057-ec93-4983-765a-acf28bbd0098"
    mock_api_post_request.return_value = {}

    with pytest.raises(RuntimeError, match="Failed to retrieve source list from API response."):
        get_resource_id(resource_type, resource_name, workspace_id)


def test_get_resource_id_api_failure(mock_api_post_request):
    # Test scenario where the API call fails or returns None
    resource_type = "connection"
    resource_name = "test-connection"
    workspace_id = "workspace-123"
    mock_api_post_request.return_value = None

    with pytest.raises(RuntimeError, match="Failed to retrieve connection list from API response."):
        get_resource_id(resource_type, resource_name, workspace_id)


###
# PYTESTS FOR add_sync_configurations() FUNCTION
###


def test_add_sync_configurations_success():
    # Test scenario where the sync catalog is properly updated
    sync_catalog = [
        {
            "stream": {
                "name": "stream1",
                "jsonSchema": {
                    "type": "object",
                    "properties": {
                        "_ab_cdc_updated_at": {"type": "string"},
                        "_ab_cdc_deleted_at": {"type": "string"},
                        "_ab_cdc_lsn": {"type": "number"},
                        "id": {"type": "string"},
                    },
                },
                "supportedSyncModes": ["full_refresh", "incremental"],
                "sourceDefinedCursor": True,
                "defaultCursorField": ["_ab_cdc_lsn"],
                "sourceDefinedPrimaryKey": [["id"]],
                "namespace": "stream1",
                "isResumable": True,
            },
            "config": {
                "syncMode": "incremental",
                "cursorField": ["_ab_cdc_lsn"],
                "destinationSyncMode": "append_dedup",
                "primaryKey": [["id"]],
                "aliasName": "stream1",
                "selected": False,
                "suggested": False,
                "selectedFields": [],
                "hashedFields": [],
                "mappers": [],
            },
        }
    ]

    result = add_sync_configurations(sync_catalog)

    expected_result = [
        {
            "stream": {
                "name": "stream1",
                "jsonSchema": {
                    "type": "object",
                    "properties": {
                        "_ab_cdc_updated_at": {"type": "string"},
                        "_ab_cdc_deleted_at": {"type": "string"},
                        "_ab_cdc_lsn": {"type": "number"},
                        "id": {"type": "string"},
                    },
                },
                "supportedSyncModes": ["full_refresh", "incremental"],
                "sourceDefinedCursor": True,
                "defaultCursorField": ["_ab_cdc_lsn"],
                "sourceDefinedPrimaryKey": [["id"]],
                "namespace": "stream1",
                "isResumable": True,
            },
            "config": {
                "syncMode": "incremental",
                "cursorField": "_ab_cdc_lsn",
                "destinationSyncMode": "append",
                "primaryKey": [["id"]],
                "aliasName": "stream1",
                "selected": True,
                "suggested": False,
                "selectedFields": [],
                "hashedFields": [],
                "mappers": [],
            },
        }
    ]

    assert result == expected_result
