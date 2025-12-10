SELECT
    toUUID(id) AS id,
    toUUID(id) AS customer_id,
    toString(username) AS username,
    toString(credentials) AS credentials,
    toString(type) AS type,
    toDateTime64(expiry, 6, 'GMT') AS expiry,
    toString(create_metadata) AS create_metadata
FROM postgresql(customer_pg_creds, table='credentials')
