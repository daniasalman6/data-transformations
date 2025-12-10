{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtractString(_airbyte_data, 'name') AS name,
    toLowCardinality(JSONExtractString(_airbyte_data, 'members_type')) AS members_type,
    JSONExtract(_airbyte_data, 'members', 'Array(UUID)') AS members,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_limit_groups') }}
