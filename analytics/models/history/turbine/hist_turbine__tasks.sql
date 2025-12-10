{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'request_id', 'UUID') AS request_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'event')) AS event,
    JSONExtractString(_airbyte_data, 'parent') AS parent,
    toLowCardinality(JSONExtractString(_airbyte_data, 'status')) AS status,
    JSONExtractBool(_airbyte_data, 'join_parent') AS join_parent,
    JSONExtractString(_airbyte_data, 'reference') AS reference,
    JSONExtractString(_airbyte_data, 'task_metadata') AS task_metadata,
    JSONExtractString(_airbyte_data, 'status_reason') AS status_reason,
    parseDateTimeBestEffort(JSONExtractString(_airbyte_data, 'create_date'), 6, 'GMT') AS create_date,
    parseDateTimeBestEffort(JSONExtractString(_airbyte_data, 'update_date'), 6, 'GMT') AS update_date,
    JSONExtract(_airbyte_data, 'executor_id', 'UUID') AS executor_id,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_tasks') }}
