SELECT
    toUUID(id) AS id,
    toUUID(customer_id) AS customer_id,
    toString(value) AS value,
    toString(key) AS key,
    toString(hash) AS hash,
    toString(permissions) AS permissions,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='attributes')
