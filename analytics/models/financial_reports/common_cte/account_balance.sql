SELECT
    splits.account_id AS account_id,
    splits.transaction_id AS txn_id,
    CASE WHEN splits.type = 'CREDIT' THEN COALESCE(splits.sum, 0) END AS credit,
    CASE WHEN splits.type = 'DEBIT' THEN COALESCE(splits.sum, 0) END AS debit,
    CASE
        WHEN splits.type = 'CREDIT' THEN COALESCE(splits.sum, 0)
        WHEN splits.type = 'DEBIT' THEN -1 * COALESCE(splits.sum, 0)
    END AS balance
FROM {{ ref('splits') }} as splits
