SELECT
    toUUID(id) AS id,
    toUUID(account_id) AS account_id,
    toString(alias) AS alias,
    toString(alias_type) AS alias_type,
    toBool(active) AS active,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(ledger_pg_creds, table='aliases')
