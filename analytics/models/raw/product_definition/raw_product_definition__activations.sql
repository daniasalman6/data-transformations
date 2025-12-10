SELECT
    toUUID(id) AS id,
    toUUID(activated_id) AS activated_id,
    toUUID(parent_id) AS parent_id,
    toString(create_metadata) AS create_metadata
FROM postgresql(prod_def_pg_creds, table='activations')
