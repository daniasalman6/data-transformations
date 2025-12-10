SELECT
    la.account_id AS account_id,
    la.txn_id AS txn_id,
    gl_id,
    debit,
    credit,
    balance,
    gla.coa_id
FROM raw_coa.raw_coa__gl_accounts gla
INNER JOIN {{ ref('account_balance') }} as la ON gla.account_id = la.account_id
