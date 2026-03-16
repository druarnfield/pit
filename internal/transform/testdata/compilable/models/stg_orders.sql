SELECT order_id, customer_id, amount
FROM raw.orders
WHERE is_active = 1
