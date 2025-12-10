-- This test ensures any failed status in status column is accompanied by a reason in the status_reason column.
{% test status_reason_not_null(model, column_name, status_column) %}
SELECT
    {{ column_name }} AS status_reason
FROM {{ model }}
WHERE {{ column_name }} IS NULL
  AND {{ status_column }} = 'FAILED'
{% endtest %}
