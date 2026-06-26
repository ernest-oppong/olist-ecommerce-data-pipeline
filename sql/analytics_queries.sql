-- 1. Revenue by Product Category
SELECT
    dp.product_category_name_english,
    ROUND(SUM(fo.total_order_value), 2) AS total_revenue
FROM fact_orders fo
JOIN dim_products dp
    ON fo.product_id = dp.product_id
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;


-- 2. Orders by Customer State
SELECT
    dc.customer_state,
    COUNT(DISTINCT fo.order_id) AS total_orders
FROM fact_orders fo
JOIN dim_customers dc
    ON fo.customer_id = dc.customer_id
GROUP BY dc.customer_state
ORDER BY total_orders DESC;


-- 3. Payment Method Distribution
SELECT
    payment_type,
    COUNT(*) AS total_payments,
    ROUND(SUM(payment_value), 2) AS total_payment_value
FROM dim_payments
GROUP BY payment_type
ORDER BY total_payment_value DESC;


-- 4. Monthly Revenue Trend
SELECT
    purchase_year,
    purchase_month,
    ROUND(SUM(total_order_value), 2) AS monthly_revenue
FROM fact_orders
GROUP BY purchase_year, purchase_month
ORDER BY purchase_year, purchase_month;


-- 5. Database Validation
SELECT 'fact_orders' AS table_name, COUNT(*) AS total_rows FROM fact_orders
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_payments', COUNT(*) FROM dim_payments;