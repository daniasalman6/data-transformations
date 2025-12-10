{{
    config(
        order_by='(update_timestamp, id)',
    )
}}
SELECT
    JSONExtract(_airbyte_data, 'id', 'UUID') AS id,
    JSONExtract(_airbyte_data, 'account_id', 'UUID') AS account_id,
    JSONExtract(_airbyte_data, 'transaction_id', 'UUID') AS transaction_id,
    toLowCardinality(JSONExtractString(_airbyte_data, 'type')) AS type,
    toLowCardinality(JSONExtractString(_airbyte_data, 'acy')) AS acy,
    toLowCardinality(JSONExtractString(_airbyte_data, 'lcy')) AS lcy,
    JSONExtract(_airbyte_data, 'acy_amount', 'Decimal(29, 9)') AS acy_amount,
    JSONExtract(_airbyte_data, 'lcy_amount', 'Decimal(29, 9)') AS lcy_amount,
    JSONExtract(_airbyte_data, 'rate', 'Decimal(76, 38)') AS rate,
    JSONExtractString(_airbyte_data, 'narration') AS narration,
    JSONExtractString(_airbyte_data, 'metadata') AS metadata,
    JSONExtractString(_airbyte_data, 'limit_dimensions') AS limit_dimensions,
    JSONExtract(_airbyte_data, 'previous_split_id', 'UUID') AS previous_split_id,
    JSONExtract(_airbyte_data, 'acy_balance_previous', 'Decimal(76, 38)') AS acy_balance_previous,
    JSONExtract(_airbyte_data, 'lcy_balance_previous', 'Decimal(76, 38)') AS lcy_balance_previous,
    JSONExtract(_airbyte_data, 'acy_balance_current', 'Decimal(76, 38)') AS acy_balance_current,
    JSONExtract(_airbyte_data, 'lcy_balance_current', 'Decimal(76, 38)') AS lcy_balance_current,
    {{ extract_cdc_updated_at('_airbyte_data') }} AS update_timestamp,
    {{ extract_cdc_deleted_at('_airbyte_data') }} AS delete_timestamp,
    _airbyte_emitted_at AS emission_timestamp,
    _airbyte_data
FROM {{ source('airbyte_tables', '_airbyte_raw_splits') }}
