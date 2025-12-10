WITH glbalances AS (
    SELECT
        gc.*,
        COUNT(a.txn_id) AS txn_count,
        SUM(a.debit) AS debit,
        SUM(a.credit) AS credit,
        SUM(a.balance) AS balance
    FROM raw_coa.raw_coa__gls gc
    LEFT JOIN {{ ref('gl_account_balance') }} as a ON gc.id = a.gl_id
    GROUP BY gc.*
    ORDER BY gc.tree DESC
),

cumbalances AS (
    SELECT
        f1.*,
        SUM(f2.txn_count) AS cumulative_count,
        SUM(f2.debit) AS cumulative_debit,
        SUM(f2.credit) AS cumulative_credit,
        SUM(f2.balance) AS cumulative_balance
    FROM glbalances f1
    CROSS JOIN glbalances f2
    WHERE f2.tree LIKE CONCAT(f1.tree, '%')
    GROUP BY f1.*
    ORDER BY f1.tree
),

results AS (
    SELECT
        gl.id as id,
        gl.tree as tree,
        splitByChar('.', gl.tree)[1] AS coa_id,
        gl.name AS name,
        p.label,
        gl.credit_increase,
        COALESCE(gl.cumulative_debit, 0) AS debit,
        COALESCE(gl.cumulative_credit, 0) AS credit,
        CASE
            WHEN gl.credit_increase = true THEN COALESCE(gl.cumulative_balance, 0)
            WHEN gl.credit_increase = false THEN -1 * COALESCE(gl.cumulative_balance, 0)
        END AS balance
    FROM cumbalances gl
    LEFT JOIN raw_coa.raw_coa__gl_aliases gla ON gl.id = gla.gl_id
    JOIN {{ ref('gl_path') }} as p ON p.id = gl.id
    WHERE p.label LIKE '%> Income%' OR p.label LIKE '%> Expenses%'
    GROUP BY
        gl.id,
        gl.tree,
        p.label,
        gl.name,
        gl.debit,
        gl.credit_increase,
        gl.cumulative_debit,
        gl.credit,
        gl.cumulative_credit,
        gl.cumulative_balance,
        coa_id
    ORDER BY tree
),

net_profit AS (
    SELECT
        CAST(NULL AS Nullable(UUID)) AS id,
        CAST(NULL AS Nullable(String)) AS tree,
        r.coa_id,
        'Net Profit' AS name,
        gl.name AS label,
        CAST(NULL AS Nullable(Bool)) AS credit_increase,
        CAST(NULL AS Nullable(Decimal(38, 19))) AS debit,
        CAST(NULL AS Nullable(Decimal(38, 19))) AS credit,
        MAX(CASE WHEN r.label LIKE '%> Income%' THEN r.balance ELSE 0 END) -
        MAX(CASE WHEN r.label LIKE '%> Expenses%' THEN r.balance ELSE 0 END) AS "Balance (MGA)"
    FROM results r
    LEFT JOIN raw_coa.raw_coa__gls gl ON r.coa_id = gl.tree
    GROUP BY r.coa_id, gl.name
)

SELECT * FROM results
UNION ALL
SELECT * FROM net_profit
