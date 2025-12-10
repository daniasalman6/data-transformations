SELECT
    toUUID(id) AS id,
    toString(scheduled_events) AS scheduled_events,
    toString(components) AS components,
    toString(requirements) AS requirements,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toString(name) AS name,
    toString(description) AS description,
    toString(type) AS type,
    toString(metadata) AS metadata,
    toUUID(family_id) AS family_id
FROM postgresql(prod_def_pg_creds, table='product_definitions')
