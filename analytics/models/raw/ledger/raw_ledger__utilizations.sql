SELECT
    toUUID(limit_id) AS limit_id,
    toString(value) AS value,
    toString(denomination) AS denomination,
    toDateTime64(last_reset, 6, 'GMT') AS last_reset
FROM postgresql(ledger_pg_creds, table='utilizations')
