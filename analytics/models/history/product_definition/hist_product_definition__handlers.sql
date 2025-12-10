{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'lang')) AS lang,
    JSONExtractString(_airbyte_data, 'handler') AS handler,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtractString(_airbyte_data, 'description') AS description,
    JSONExtractBool(_airbyte_data, 'is_static') AS is_static,
    JSONExtractString(_airbyte_data, 'inputs') AS inputs,
    JSONExtractString(_airbyte_data, 'request_metadata') AS request_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_handlers') }}
