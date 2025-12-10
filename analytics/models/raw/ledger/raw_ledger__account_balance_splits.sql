SELECT
    toUUID(account_id) AS account_id,
    toUUID(split_id) AS split_id
FROM postgresql(ledger_pg_creds, table='account_balance_splits')
