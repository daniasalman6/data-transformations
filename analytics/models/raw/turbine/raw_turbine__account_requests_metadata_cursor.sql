SELECT
    toUUID(request_id) AS request_id,
    toUUID(metadata_id) AS metadata_id
FROM postgresql(turbine_pg_creds, table='account_requests_metadata_cursor')
