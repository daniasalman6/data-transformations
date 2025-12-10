SELECT
    toUUID(id) AS id,
    toString(entity_type) AS entity_type,
    toUUID(entity_id) AS entity_id,
    toString(tag) AS tag,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toDateTime64(active_from, 6, 'GMT') AS active_from,
    toDateTime64(active_to, 6, 'GMT') AS active_to
FROM postgresql(tagging_pg_creds, table='tagging')
