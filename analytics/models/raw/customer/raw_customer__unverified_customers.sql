SELECT
    toUUID(id) AS id,
    toString(sign_in_attribute_hash) AS sign_in_attribute_hash,
    toInt32(attempts) AS attempts,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='unverified_customers')
