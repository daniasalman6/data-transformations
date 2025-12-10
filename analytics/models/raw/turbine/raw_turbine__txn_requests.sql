{{
    config(
        materialized = "view",
    )
}}
SELECT
    toUUID(id) AS id,
    toString(txn_code) AS txn_code,
    toString(from_account) AS from_account,
    toString(from_account_ref_type) AS from_account_ref_type,
    toString(to_account) AS to_account,
    toString(to_account_ref_type) AS to_account_ref_type,
    toString(currency) AS currency,
    CAST(amount AS Decimal(29, 9)) AS amount,
    toString(event_trace) AS event_trace,
    toString(direction) AS direction,
    toUUID(associated_txn_request) AS associated_txn_request,
    toString(associated_txn_type) AS associated_txn_type,
    toString(request_metadata) AS request_metadata,
    toString(create_metadata) AS create_metadata,
    toUUID(batch_id) AS batch_id,
    toString(reference) AS reference
FROM postgresql(turbine_pg_creds, table='txn_requests')
