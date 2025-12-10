{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'request_id', 'UUID') AS request_id,
    JSONExtractString(_airbyte_data, 'reference') AS reference,
    JSONExtractString(_airbyte_data, 'narration') AS narration,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_transactions') }}
