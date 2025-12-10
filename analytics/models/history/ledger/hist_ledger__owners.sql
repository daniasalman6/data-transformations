{{
    config(
        order_by='(update_timestamp, owner_id, account_id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'account_id', 'UUID') AS account_id,
    JSONExtract(_airbyte_data, 'owner_id', 'UUID') AS owner_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'owner_type')) AS owner_type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'owner_category')) AS owner_category,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_owners') }}
