SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM {{ ref "stg_orders" }} o
