SELECT *
FROM executable(
    'decrypt_attributes.py',
    TabSeparated,
    'customer_id UUID, attributes String',
    (
        SELECT
            customer_id,
            toJSONString(groupArray(key)) AS encrypted_attr
        FROM {{ ref('raw_customer__attributes') }}
        WHERE hash != ''
        GROUP BY customer_id
    ),
    SETTINGS
        command_termination_timeout = {{ var('command_termination_timeout') }},
        command_read_timeout = {{ var('command_read_timeout') }},
        command_write_timeout = {{ var('command_write_timeout') }}
)
