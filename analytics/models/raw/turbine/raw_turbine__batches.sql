SELECT
    toUUID(id) AS id,
    toString(file_path) AS file_path,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(turbine_pg_creds, table='batches')
