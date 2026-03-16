SELECT order_date, SUM(amount) as total FROM raw.orders GROUP BY order_date
