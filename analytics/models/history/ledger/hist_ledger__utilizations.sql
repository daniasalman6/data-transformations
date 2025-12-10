{{
    config(
        order_by='(update_timestamp, limit_id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'limit_id', 'UUID') AS limit_id,
    JSONExtract(_airbyte_data, 'value', 'Decimal(76, 38)') AS value,
    toLowCardinality(JSONExtractString(_airbyte_data, 'denomination')) AS denomination,
    parseDateTimeBestEffort(JSONExtractString(_airbyte_data, 'last_reset'), 6, 'GMT') AS last_reset,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_utilizations') }}
