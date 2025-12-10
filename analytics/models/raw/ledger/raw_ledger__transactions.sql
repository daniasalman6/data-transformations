SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(reference) AS reference,
    toString(narration) AS narration,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata
FROM postgresql(ledger_pg_creds, table='transactions')
