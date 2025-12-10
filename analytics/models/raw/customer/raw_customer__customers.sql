SELECT
    toUUID(id) AS id,
    toString(status) AS status,
    toString(status_metadata) AS status_metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='customers')
