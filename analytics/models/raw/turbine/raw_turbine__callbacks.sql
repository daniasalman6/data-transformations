SELECT
    toUUID(id) AS id,
    toUUID(request_id) AS request_id,
    toString(type) AS type,
    toString(type) AS details,
    toString(metadata) AS metadata,
    toString(create_metadata) AS create_metadata
FROM postgresql(turbine_pg_creds, table='callbacks')
