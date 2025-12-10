SELECT
    toUUID(id) AS id,
    toUUID(from_product) AS from_product,
    toUUID(to_product) AS to_product,
    toString(status) AS status,
    toString(status_reason) AS status_reason,
    toString(request_metadata) AS request_metadata,
    toString(migration_metadata) AS migration_metadata,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(turbine_pg_creds, table='migration_requests')
