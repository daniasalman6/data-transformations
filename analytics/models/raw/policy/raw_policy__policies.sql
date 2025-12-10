SELECT
    toString(hash) AS hash,
    toUUID(id) AS id,
    toString(type) AS type,
    toString(lang) AS lang,
    toString(execution_strategy) AS execution_strategy,
    toString(name) AS name,
    toString(description) AS description,
    toString(rules) AS rules,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(policy_pg_creds, table='policies')
