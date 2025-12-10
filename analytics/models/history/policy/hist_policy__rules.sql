{{
    config(
        order_by='(update_timestamp, hash)',
    )
}}
SELECT
    JSONExtractString(_airbyte_data, 'hash') AS hash,
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'policy_id', 'UUID') AS policy_id,
    JSONExtractString(_airbyte_data, 'name') AS name,
    JSONExtractString(_airbyte_data, 'description') AS description,
    JSONExtractString(_airbyte_data, 'predicate') AS predicate,
    JSONExtractString(_airbyte_data, 'consequences') AS consequences,
    JSONExtractString(_airbyte_data, 'create_metadata') AS create_metadata,
    JSONExtractString(_airbyte_data, 'update_metadata') AS update_metadata,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_rules') }}
