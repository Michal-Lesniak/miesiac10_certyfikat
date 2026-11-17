-- Singular test: nie powinno być zamówień z datą w przyszłości.
SELECT order_id, created_at
FROM {{ ref('silver_orders') }}
WHERE created_at > CURRENT_TIMESTAMP()
