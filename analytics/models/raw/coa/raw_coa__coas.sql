SELECT
    toUUID(id) AS id,
    toString(base_currency) AS base_currency
FROM postgresql(coa_pg_creds, table='coas')
