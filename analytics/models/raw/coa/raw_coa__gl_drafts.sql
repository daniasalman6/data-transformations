SELECT
    toUUID(account_id) AS account_id,
    toUUID(gl_id) AS gl_id,
    toUUID(coa_id) AS coa_id,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(coa_pg_creds, table='gl_drafts')
