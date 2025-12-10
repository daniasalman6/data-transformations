-- this macro is used to validate if the values in a certain column are greater than or equal to 0.
{% test assert_value_greater_than_zero(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} <= 0

{% endtest %}
