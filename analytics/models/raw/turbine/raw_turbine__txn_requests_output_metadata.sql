SELECT
    toUUID(request_id) AS request_id,
    toString(output_metadata) AS output_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(turbine_pg_creds, table='txn_requests_output_metadata')
