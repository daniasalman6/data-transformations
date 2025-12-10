{{
    config(
        order_by='(update_timestamp, gl_id, alias)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'gl_id', 'UUID') AS gl_id,
    JSONExtractString(_airbyte_data, 'alias') AS alias,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_gl_aliases') }}
