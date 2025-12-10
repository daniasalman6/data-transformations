SELECT
    toUUID(id) AS id,
    toUUID(owner_id) AS owner_id,
    toString(key) AS key,
    toString(hash) AS hash,
    toInt32(setup_attempts) AS setup_attempts,
    toInt32(unsuccessful_validation_attempts) AS unsuccessful_validation_attempts,
    toInt32(successful_validation_attempts) AS successful_validation_attempts,
    toString(context) AS context
FROM postgresql(customer_pg_creds, table='sign_in_attribute_verifications')
