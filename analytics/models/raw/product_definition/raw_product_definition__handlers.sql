SELECT
    toUUID(id) AS id,
    toString(type) AS type,
    toString(lang) AS lang,
    toString(handler) AS handler,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toString(name) AS name,
    toString(description) AS description,
    toBool(is_static) AS is_static,
    toString(inputs) AS inputs,
    toString(request_metadata) AS request_metadata
FROM postgresql(prod_def_pg_creds, table='handlers')
