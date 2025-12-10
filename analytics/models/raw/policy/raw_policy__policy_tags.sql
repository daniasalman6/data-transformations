SELECT
    toUUID(policy_id) AS policy_id,
    toString(tag) AS tag,
    toString(hash) AS hash
FROM postgresql(policy_pg_creds, table='policy_tags')
