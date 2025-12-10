SELECT
    toUUID(request_id) AS request_id,
    toUUID(status_id) AS status_id
FROM postgresql(turbine_pg_creds, table='txn_requests_status_cursor')
