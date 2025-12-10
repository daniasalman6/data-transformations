from typing import Optional, List
import requests
import requests.auth
from ..configs import airbyte as ab_conf
from dagster import get_dagster_logger


logger = get_dagster_logger()


def api_post_request(api_endpoint: str, payload: dict):
    """Sends a POST request to the specified Airbyte API endpoint.

    Args:
        api_endpoint (str): The API endpoint to which the request is sent.
        payload (dict): The JSON payload to include in the request.

    Returns:
        dict: The JSON response from the API if the request is successful.

    Exceptions:
        Raises a RuntimeError if the API request fails due to network issues or an unsuccessful status code.
    """
    url = f"{ab_conf.AIRBYTE_API_URL}{api_endpoint}"

    try:
        logger.info(f"Making a POST request to: {url}")
        response = requests.post(
            url=url, auth=ab_conf.API_AUTH, headers=ab_conf.API_REQUEST_HEADER, json=payload, verify=False
        )

        if response.status_code == 200:
            data = response.json()
            logger.info(
                f"API request returned successfully. Status code: {response.status_code}, Success: {response.text}"
            )
            return data
        else:
            logger.error(
                f"Error in making API request. Status code: {response.status_code}, Error: {response.text}",
                exc_info=True,
            )
            response.raise_for_status()

    except requests.RequestException as e:
        raise RuntimeError(f"An unexpected error ocurred: {e}")


def get_workspace_id() -> str:
    """Attempts to retrieve the workspace ID from Airbyte.

    Returns:
        str: The workspace ID if successfully fetched, or None if an error occurs.

    Exceptions:
        Logs an error if the workspace ID is not found in the response
    """

    workspaces_data = api_post_request("/workspaces/list", payload={})

    if workspaces_data and workspaces_data.get("workspaces"):
        logger.info("Workspace ID fetched successfully")
        return workspaces_data["workspaces"][0]["workspaceId"]
    else:
        raise RuntimeError("No workspaces found in the response.")


def create_airbyte_resource(resource_type: str, workspace_id: str, resource_name: str, payload: dict):
    """
    Sets up an Airbyte source, destination or connection with user-defined configurations.

    Args:
        resource_type: The type of resource ('source', 'destination' or 'connection').
        workspace_id: The ID of the workspace where the resource should be created.
        resource_name: The name of the resource.
        payload: The request payload for creating the resource.

    Raises:
        RuntimeError: If the API request to create the resource fails.
    """
    if resource_type not in ["source", "destination", "connection"]:
        logger.error(f"Invalid resource_type '{resource_type}'. Must be 'source', 'destination or 'connection'.")
        return

    existing_resource_id = get_resource_id(
        resource_type=resource_type, resource_name=resource_name, workspace_id=workspace_id
    )

    if existing_resource_id:
        logger.info(f"{resource_type.capitalize()} '{resource_name}' already exists.")
        return

    endpoint = f"/{resource_type}s/create"
    response = api_post_request(endpoint, payload=payload)

    if not response:
        raise RuntimeError(f"Failed to set up {resource_type} '{resource_name}'.")


def discover_schema(source_id: Optional[str], streams: List[str]) -> list:
    """Attempts to retrieve the streams and schema of the data to sync from a specified source.

    Args:
        source_id: The ID of the source from which streams are being retrieved, obtained using get_resource_id().
        streams: A list of stream or table names (as strings) to sync.

    Returns:
        list: A list of streams in a format compatible with Airbyte, including their sync modes, cursor fields, and JSON schema.

    Exceptions:
        Logs an error if the API request fails, if no streams are found in the specified source,
        or if any other unexpected exception occurs.
    """
    payload = {"sourceId": source_id}

    streams_data = api_post_request("/sources/discover_schema", payload=payload)

    if not streams_data or "catalog" not in streams_data:
        raise RuntimeError("Failed to retrieve catalog from source discovery response.")

    full_catalog = streams_data.get("catalog", {})
    filtered_streams = [
        stream for stream in full_catalog.get("streams", []) if stream.get("stream", {}).get("name") in streams
    ]
    if not filtered_streams:
        raise RuntimeError("Error: Streams not found in source catalog.")

    logger.info(f"Filtered streams: {filtered_streams}")
    return filtered_streams


def get_resource_id(resource_type: str, resource_name: str, workspace_id: str) -> Optional[str]:
    """
    Retrieves the ID of a specified resource (source, destination or connection) based on its name.

    Args:
        resource_type: The type of resource ('source', 'destination' or 'connection').
        resource_name: The name of the resource whose ID is being fetched.
        workspace_id: The ID of the workspace where the resource is located.

    Returns:
        Optional[str]: The ID of the resource associated with the given name, or None if not found.
    """
    if resource_type not in ["source", "destination", "connection"]:
        raise ValueError(f"Invalid resource_type '{resource_type}'. Must be 'source', 'destination' or 'connection'.")

    payload = {"workspaceId": workspace_id}
    endpoint = f"/{resource_type}s/list"

    resources_data = api_post_request(endpoint, payload=payload)

    if not resources_data:
        raise RuntimeError(f"Failed to retrieve {resource_type} list from API response.")

    resource_id = ""
    for resource in resources_data.get(f"{resource_type}s", []):
        if resource.get("name") == resource_name:
            logger.info(f"Successfully retrieved {resource_type} ID for {resource_name}.")
            resource_id = resource.get(f"{resource_type}Id")
    if resource_id:
        return resource_id
    else:
        return None


def add_sync_configurations(sync_catalog: list) -> list:
    """
    Updates the sync configurations for each stream in the given sync catalog.

    Args:
        sync_catalog: A list of stream dictionaries to configure.

    Returns:
        Optional[list]: The updated sync catalog or None if an error occurs.
    """

    updated_catalog = []

    for stream in sync_catalog:
        stream["config"]["syncMode"] = "incremental"
        stream["config"]["cursorField"] = "_ab_cdc_lsn"
        stream["config"]["destinationSyncMode"] = "append"
        stream["config"]["selected"] = True
        updated_catalog.append(stream)

    logger.info("Successfully updated sync configurations for all streams.")
    return updated_catalog
