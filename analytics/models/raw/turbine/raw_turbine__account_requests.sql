SELECT
    toUUID(id) AS id,
    toString(create_metadata) AS create_metadata,
    toString(title) AS title,
    toString(currency) AS currency,
    toUUID(product_id) AS product_id,
    toString(owners) AS owners,
    toString(reference) AS reference,
    toString(operational_instruction) AS operational_instruction
FROM postgresql(turbine_pg_creds, table='account_requests')
