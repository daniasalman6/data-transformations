{{
    config(
        order_by='(update_timestamp, type)',
    )
}}
SELECT
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    JSONExtractBool(_airbyte_data, 'encrypted') AS encrypted,
    JSONExtractString(_airbyte_data, 'salt') AS salt,
    toLowCardinality(JSONExtractString(_airbyte_data, 'hash_function')) AS hash_function,
    JSONExtractString(_airbyte_data, 'validation_regex') AS validation_regex,
    JSONExtractString(_airbyte_data, 'allowed_values') AS allowed_values,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_type_definitions') }}
