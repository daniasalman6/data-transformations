SELECT
    toUUID(id) AS id,
    toString(name) AS name,
    CAST(members AS Array(UUID)) AS members,
    toString(members_type) AS members_type,
    toString(create_metadata) AS create_metadata
FROM postgresql(ledger_pg_creds, table='limit_groups')
