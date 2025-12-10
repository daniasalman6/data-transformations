{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'customer_id', 'UUID') AS customer_id,
    JSONExtractString(_airbyte_data, 'username') AS username,
    JSONExtractString(_airbyte_data, 'credentials') AS credentials,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    parseDateTimeBestEffortOrNull(JSONExtractString(_airbyte_data, 'expiry'), 6, 'GMT') AS expiry,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_credentials') }}
