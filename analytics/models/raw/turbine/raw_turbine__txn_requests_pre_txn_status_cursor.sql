SELECT
    toUUID(request_id) AS request_id,
    toUUID(pre_txn_status_id) AS pre_txn_status_id
FROM postgresql(turbine_pg_creds, table='txn_requests_pre_txn_status_cursor')
