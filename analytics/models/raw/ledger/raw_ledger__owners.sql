SELECT
    toUUID(account_id) AS account_id,
    toUUID(owner_id) AS owner_id,
    toString(owner_type) AS owner_type,
    toString(owner_category) AS owner_category,
    toString(create_metadata) AS create_metadata
FROM postgresql(ledger_pg_creds, table='owners')
