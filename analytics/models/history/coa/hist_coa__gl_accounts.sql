{{
    config(
        order_by='(update_timestamp, account_id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'account_id', 'UUID') AS account_id,
    JSONExtract(_airbyte_data, 'gl_id', 'UUID') AS gl_id,
    JSONExtract(_airbyte_data, 'coa_id', 'UUID') AS coa_id,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_gl_accounts') }}
