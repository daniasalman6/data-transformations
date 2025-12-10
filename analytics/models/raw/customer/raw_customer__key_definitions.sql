SELECT
    toString(key) AS key,
    toString(type) As type,
    toString(permissions) AS permissions,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='key_definitions')
