SELECT
    toString(type) AS type,
    toBool(encrypted) AS encrypted,
    toString(salt) AS salt,
    toString(hash_function) AS hash_function,
    toString(validation_regex) AS validation_regex,
    toString(allowed_values) AS allowed_values,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(customer_pg_creds, table='type_definitions')
