SELECT
    toUUID(id) AS id,
    toUUID(account_id) AS account_id,
    toUUID(migration_id) AS migration_id,
    toString(status) AS status,
    toString(status_reason) AS status_reason,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(turbine_pg_creds, table='account_migration_requests')
