{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'from_product', 'UUID') AS from_product,
    JSONExtract(_airbyte_data, 'to_product', 'UUID') AS to_product,
    toLowCardinality(JSONExtractString(_airbyte_data, 'status')) AS status,
    JSONExtractString(_airbyte_data, 'status_reason') AS status_reason,
    JSONExtractString(_airbyte_data, 'request_metadata') AS request_metadata,
    JSONExtractString(_airbyte_data, 'migration_metadata') AS migration_metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_migration_requests') }}
