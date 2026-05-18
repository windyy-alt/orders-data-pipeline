-- Daily Order & Revenue Proxy
SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(product_id) AS total_items_sold
FROM analytics.orders_products;

-- Customer Segmentation Breakdown
SELECT 
    customer_segment,
    COUNT(DISTINCT user_id) AS total_customers
FROM analytics.mart_rfm
GROUP BY customer_segment
ORDER BY total_customers DESC;

-- RFM Value Contribution
SELECT 
    customer_segment,
    SUM(monetary_volume) AS total_items_purchased,
    AVG(recency_avg_days) AS avg_return_days
FROM analytics.mart_rfm
GROUP BY customer_segment
ORDER BY total_items_purchased DESC;

-- Department Sales Contribution (Bar)
SELECT 
    department,
    COUNT(product_id) AS total_sold
FROM analytics.orders_products
GROUP BY department
ORDER BY total_sold DESC
LIMIT 15;

-- High Reorder Rate
SELECT 
    product_name,
    department,
    COUNT(order_id) AS total_purchases,
    SUM(reordered) AS total_reorders,
    ROUND(SUM(reordered) * 100.0 / COUNT(order_id), 2) AS reorder_rate_percentage
FROM analytics.orders_products
GROUP BY product_name, department
ORDER BY reorder_rate_percentage DESC
LIMIT 10;

-- Global Platform Stickiness
SELECT 
    ROUND(SUM(reordered) * 100.0 / COUNT(order_id), 2) AS global_reorder_rate_percentage
FROM analytics.orders_products;
