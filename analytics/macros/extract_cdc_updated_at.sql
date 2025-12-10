{%- macro extract_cdc_updated_at(json_data) -%}
    parseDateTimeBestEffortOrZero(JSONExtractString({{ json_data }}, '_ab_cdc_updated_at'), 9, 'GMT')
{%- endmacro -%}
