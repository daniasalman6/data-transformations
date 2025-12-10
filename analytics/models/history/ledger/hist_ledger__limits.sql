{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'entity_id', 'UUID') AS entity_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'entity_type')) as entity_type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'context')) as context,
    JSONExtractString(_airbyte_data, 'limit_metadata') AS limit_metadata,
    JSONExtractString(_airbyte_data, 'dimensions') AS dimensions,
    JSONExtract(_airbyte_data, 'value', 'Decimal(76, 38)') AS value,
    toLowCardinality(JSONExtractString(_airbyte_data, 'denomination')) AS denomination,
    toLowCardinality(JSONExtractString(_airbyte_data, 'utilization_change')) AS utilization_change,
    JSONExtract(_airbyte_data, 'upper_tolerance', 'Decimal(76, 38)') AS upper_tolerance,
    JSONExtract(_airbyte_data, 'lower_tolerance', 'Decimal(76, 38)') AS lower_tolerance,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_limits') }}
