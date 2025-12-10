{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'entity_type')) AS entity_type,
    JSONExtract(_airbyte_data, 'entity_id', 'UUID') AS entity_id,
    JSONExtractString(_airbyte_data, 'tag') AS tag,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    parseDateTimeBestEffortOrNull(JSONExtractString(_airbyte_data, 'active_from'), 6, 'GMT') AS active_from,
    parseDateTimeBestEffortOrNull(JSONExtractString(_airbyte_data, 'active_to'), 6, 'GMT') AS active_to,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_tagging') }}
