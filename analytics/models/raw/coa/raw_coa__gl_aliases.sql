SELECT
       toUUID(gl_id) AS gl_id,
       toString(alias) AS alias,
       toString(create_metadata) AS create_metadata
FROM postgresql(coa_pg_creds, table='gl_aliases')
