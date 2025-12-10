{%- macro human_readable_timestamp(json_data) -%}

    to_timestamp(({{ json_data }} ->> 'time')::float)

{%- endmacro -%}
