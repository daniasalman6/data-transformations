SELECT
    toString(hash) AS hash,
    toUUID(id) AS id,
    toUUID(policy_id) AS policy_id,
    toString(name) AS name,
    toString(description) AS description,
    toString(predicate) AS predicate,
    toString(consequences) AS consequences,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata
FROM postgresql(policy_pg_creds, table='rules')
