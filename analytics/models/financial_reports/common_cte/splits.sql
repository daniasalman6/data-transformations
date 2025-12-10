SELECT
    id,
    account_id,
    lcy_amount AS sum,
    type,
    transaction_id,
    acy,
    lcy,
    acy_amount,
    rate
FROM raw_ledger.raw_ledger__splits
