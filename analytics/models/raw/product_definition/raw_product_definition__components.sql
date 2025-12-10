SELECT
    toUUID(id) AS id,
    toString(event) AS event,
    toUUID(handler_id) AS handler_id,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toString(name) AS name,
    toString(description) AS description
FROM postgresql(prod_def_pg_creds, table='components')
