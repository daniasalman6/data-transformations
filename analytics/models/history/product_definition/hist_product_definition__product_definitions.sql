{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtractString(_airbyte_data, 'scheduled_events') AS scheduled_events,
    JSONExtractString(_airbyte_data, 'components') AS components,
    JSONExtractString(_airbyte_data, 'requirements') AS requirements,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtractString(_airbyte_data, 'description') AS description,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtract(_airbyte_data, 'family_id', 'UUID') AS family_id,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_product_definitions') }}
