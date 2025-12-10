SELECT
    toUUID(id) AS id,
    toString(tree) AS tree,
    toString(name) AS name,
    toString(description) AS description,
    toBool(credit_increase) AS credit_increase,
    toString(create_metadata) AS create_metadata,
    toString(update_metadata) AS update_metadata,
    toString(status) AS status
FROM postgresql(coa_pg_creds, table='gls')
