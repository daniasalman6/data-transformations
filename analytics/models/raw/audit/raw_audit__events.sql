SELECT
    toUUID(id) AS id,
    toDateTime64(created_at, 6, 'GMT') AS created_at,
    toString(name) AS name,
    toString(affected_id) AS affected_id,
    toString(affected_type) AS affected_type,
    toUUID(actor_id) AS actor_id,
    toString(actor_type) AS actor_type,
    toString(tags) AS tags,
    toString(metadata) AS metadata,
    toString(description) AS description,
    toString(source) AS source
FROM postgresql(audit_pg_creds, table='events')
