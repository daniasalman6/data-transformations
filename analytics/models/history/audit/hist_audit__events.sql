{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    parseDateTimeBestEffortOrNull(JSONExtractString(_airbyte_data, 'created_at'), 6, 'GMT') AS created_at,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtractString(_airbyte_data, 'affected_id') AS affected_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'affected_type')) AS affected_type,
    JSONExtract(_airbyte_data, 'actor_id', 'UUID') AS actor_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'actor_type')) AS actor_type,
    JSONExtractString(_airbyte_data, 'tags') AS tags,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'description') AS description,
    toLowCardinality(JSONExtractString(_airbyte_data, 'source')) AS source,
    JSONExtractString(_airbyte_data, 'created_at') AS created_at,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_events') }}
