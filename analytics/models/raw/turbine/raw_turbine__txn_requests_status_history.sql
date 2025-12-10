SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(status) AS status,
    toString(status_reason) AS status_reason,
    toString(create_metadata) AS create_metadata
FROM postgresql(turbine_pg_creds, table='txn_requests_status_history')
