SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(pre_txn_status) AS pre_txn_status,
    toString(pre_txn_status_reason) AS pre_txn_status_reason,
    toString(create_metadata) AS create_metadata
FROM postgresql(turbine_pg_creds, table='txn_requests_pre_txn_status_history')
