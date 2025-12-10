{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtractString(_airbyte_data, 'request_metadata') AS request_metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'status') AS status,
    JSONExtractString(_airbyte_data, 'status_reason') AS status_reason,
    JSONExtractString(_airbyte_data, 'title') AS title,
    toLowCardinality(JSONExtractString(_airbyte_data, 'currency')) AS currency,
    JSONExtract(_airbyte_data, 'product_id', 'UUID') AS product_id,
    JSONExtractString(_airbyte_data, 'owners') AS owners,
    JSONExtractString(_airbyte_data, 'process_metadata') AS process_metadata,
    JSONExtractString(_airbyte_data, 'reference') AS reference,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_account_requests') }}
