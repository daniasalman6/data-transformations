SELECT
    toUUID(id) AS id,
    toBool(requested) AS requested,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(coa_pg_creds, table='draft_accounts')
