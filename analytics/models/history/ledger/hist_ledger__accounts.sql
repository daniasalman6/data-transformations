{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'status')) AS status,
    toLowCardinality(JSONExtractString(_airbyte_data, 'currency')) AS currency,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtract(_airbyte_data, 'product_id', 'UUID') AS product_id,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'reference') AS reference,
    JSONExtract(_airbyte_data, 'coa_id', 'UUID') AS coa_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'lcy')) AS lcy,
    JSONExtractString(_airbyte_data, 'limit_dimensions') AS limit_dimensions,
    JSONExtractString(_airbyte_data, 'account_metadata_schema') AS account_metadata_schema,
    JSONExtract(_airbyte_data, 'revision', 'UUID') AS revision,
    toLowCardinality(JSONExtractString(_airbyte_data, 'operational_instruction')) AS operational_instruction,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_accounts') }}
