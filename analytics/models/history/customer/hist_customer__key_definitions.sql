{{
    config(
        order_by='(update_timestamp, key)',
    )
}}
SELECT
    toLowCardinality(JSONExtractString(_airbyte_data, 'key')) AS key,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'permissions')) AS permissions,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_key_definitions') }}
