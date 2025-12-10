{{
    config(
        order_by='(update_timestamp, hash)',
    )
}}
SELECT
    JSONExtractString(_airbyte_data, 'hash') AS hash,
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'lang')) AS lang,
    toLowCardinality(JSONExtractString(_airbyte_data, 'execution_strategy')) AS execution_strategy,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtractString(_airbyte_data, 'description') AS description,
    JSONExtractString(_airbyte_data, 'rules') AS rules,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_policies') }}
