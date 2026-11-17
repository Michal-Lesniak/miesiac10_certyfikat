-- Custom generic test — kolumna numeryczna musi być >= 0.
-- Użycie w schema.yml:
--   tests:
--     - positive_or_zero
{% test positive_or_zero(model, column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} < 0

{% endtest %}
