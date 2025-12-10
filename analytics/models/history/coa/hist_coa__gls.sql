{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtractString(_airbyte_data, 'tree') AS tree,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtract(_airbyte_data, 'description', 'TEXT') AS description,
    toLowCardinality(JSONExtractBool(_airbyte_data, 'credit_increase')) AS credit_increase,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'status') AS status,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_gls') }}
