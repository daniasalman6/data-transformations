SELECT
    toUUID(id) AS id,
    toString(type) AS type,
    toString(status) AS status,
    toString(currency) AS currency,
    toString(name) AS name,
    toUUID(product_id) AS product_id,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toString(reference) AS reference,
    toUUID(coa_id) AS coa_id,
    toString(lcy) AS lcy,
    toString(limit_dimensions) AS limit_dimensions,
    toString(account_metadata_schema) AS account_metadata_schema,
    toUUID(revision) AS revision,
    toString(operational_instruction) AS operational_instruction
FROM postgresql(ledger_pg_creds, table='accounts')
