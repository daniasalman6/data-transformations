{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'txn_code')) AS txn_code,
    JSONExtractString(_airbyte_data, 'from_account') AS from_account,
    toLowCardinality(JSONExtractString(_airbyte_data, 'from_account_ref_type')) AS from_account_ref_type,
    JSONExtractString(_airbyte_data, 'to_account') AS to_account,
    toLowCardinality(JSONExtractString(_airbyte_data, 'to_account_ref_type')) AS to_account_ref_type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'currency')) AS currency,
    JSONExtract(_airbyte_data, 'amount', 'Decimal(29, 9)') AS amount,
    JSONExtractString(_airbyte_data, 'event_trace') AS event_trace,
    toLowCardinality(JSONExtractString(_airbyte_data, 'direction')) AS direction,
    toLowCardinality(JSONExtractString(_airbyte_data, 'status')) AS status,
    JSONExtractString(_airbyte_data, 'status_reason') AS status_reason,
    JSONExtract(_airbyte_data, 'associated_txn_request', 'UUID') AS associated_txn_request,
    JSONExtractString(_airbyte_data, 'associated_txn_type') AS associated_txn_type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'pre_txn_status')) AS pre_txn_status,
    JSONExtract(_airbyte_data, 'batch_id', 'UUID') AS batch_id,
    JSONExtractString(_airbyte_data, 'reference') AS reference,
    JSONExtractString(_airbyte_data, 'request_metadata') AS request_metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'process_metadata') AS process_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_txn_requests') }}
