SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(request_metadata) AS request_metadata,
    toString(create_metadata) AS create_metadata
FROM postgresql(turbine_pg_creds, table='account_requests_metadata_history')
