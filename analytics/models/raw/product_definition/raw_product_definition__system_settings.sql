SELECT
    toUUID(id) AS id,
    toString(type) AS type,
    toString(value) AS value,
    toBool(active) AS active,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(prod_def_pg_creds, table='system_settings')
