SELECT
    toUUID(id) AS id,
    toUUID(customer_id) AS customer_id,
    toUUID(unverified_customer_id) AS unverified_customer_id,
    toString(key) AS key,
    toString(value) AS value,
    toString(hash) AS hash,
    toBool(verified) AS verified,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='sign_in_attributes')
