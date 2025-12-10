SELECT
    toUUID(id) AS id,
    toString(name) AS name,
    toString(namespace) AS namespace,
    toString(language) AS language,
    toString(value) AS value,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(parameter_pg_creds, table='parameters')
