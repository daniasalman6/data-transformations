{%- macro extract_cdc_deleted_at(json_data) -%}
    CASE
        WHEN JSONExtractString({{ json_data }}, '_ab_cdc_deleted_at') IS NULL THEN NULL
        ELSE parseDateTimeBestEffortOrNull(JSONExtractString({{ json_data }}, '_ab_cdc_deleted_at'), 9, 'GMT')
    END
{%- endmacro -%}
