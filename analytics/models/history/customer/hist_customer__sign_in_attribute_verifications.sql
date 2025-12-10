{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'owner_id', 'UUID') AS owner_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'key')) AS key,
    JSONExtractString(_airbyte_data, 'hash') AS hash,
    JSONExtractInt(_airbyte_data, 'setup_attempts') AS setup_attempts,
    JSONExtractInt(_airbyte_data, 'unsuccessful_validation_attempts') AS unsuccessful_validation_attempts,
    JSONExtractInt(_airbyte_data, 'successful_validation_attempts') AS successful_validation_attempts,
    JSONExtractString(_airbyte_data, 'context') AS context,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_sign_in_attribute_verifications') }}
