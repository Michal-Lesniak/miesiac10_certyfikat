-- Singular test: amount zamówienia musi zgadzać się z sumą pozycji w order_items.
{{ config(severity = 'error') }}

WITH items AS (
    SELECT order_id, SUM(line_total) AS items_total
    FROM {{ source('bronze', 'order_items') }}
    GROUP BY order_id
),
check_set AS (
    SELECT
        o.order_id,
        o.amount,
        COALESCE(i.items_total, 0) AS items_total,
        ABS(o.amount - COALESCE(i.items_total, 0)) AS diff
    FROM {{ ref('silver_orders') }} o
    LEFT JOIN items i USING (order_id)
)
SELECT *
FROM check_set
WHERE diff > 0.01
