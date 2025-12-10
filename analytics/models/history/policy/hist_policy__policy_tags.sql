{{
    config(
        order_by='(update_timestamp, policy_id, tag)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'policy_id', 'UUID') AS policy_id,
    JSONExtractString(_airbyte_data, 'tag') AS tag,
    JSONExtractString(_airbyte_data, 'hash') AS hash,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_policy_tags') }}
