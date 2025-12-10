SELECT
    toUUID(id) AS id,
    toUUID(account_id) AS account_id,
    toUUID(transaction_id) AS transaction_id,
    toString(type) AS type,
    toString(acy) AS acy,
    toString(lcy) AS lcy,
    CAST(acy_amount AS Decimal(29, 9)) AS acy_amount,
    CAST(lcy_amount AS Decimal(29, 9)) AS lcy_amount,
    toDecimal256(rate, 38) AS rate,
    toString(narration) AS narration,
    toString(metadata) AS metadata,
    toString(limit_dimensions) AS limit_dimensions,
    toUUID(previous_split_id) AS previous_split_id,
    toDecimal256(acy_balance_previous, 38) AS acy_balance_previous,
    toDecimal256(lcy_balance_previous, 38) AS lcy_balance_previous,
    toDecimal256(acy_balance_current, 38) AS acy_balance_current,
    toDecimal256(lcy_balance_current, 38) AS lcy_balance_current
FROM postgresql(ledger_pg_creds, table='splits')
