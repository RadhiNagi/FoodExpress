-- ============================================================
-- FOODEXPRESS — Sample Analytics Queries (Gold Layer)
-- These prove the Star Schema works for real business questions
-- ============================================================


-- Q1: Revenue by restaurant and cuisine
-- Business question: "Which restaurants make the most money?"
SELECT
    r.restaurant_name,
    r.cuisine_type,
    COUNT(*) AS total_orders,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip
FROM fact_orders f
JOIN dim_restaurant r ON f.restaurant_id = r.restaurant_id
GROUP BY r.restaurant_name, r.cuisine_type
ORDER BY total_revenue DESC;


-- Q2: Delivery performance by zone
-- Business question: "Which zones have the slowest deliveries?"
SELECT
    l.zone_name,
    l.city,
    l.region,
    COUNT(*) AS total_orders,
    ROUND(AVG(f.delivery_time_minutes), 1) AS avg_delivery_min,
    ROUND(AVG(f.prep_time_minutes), 1) AS avg_prep_min,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact_orders f
JOIN dim_location l ON f.zone_id = l.zone_id AND l.is_current = true
WHERE f.rating IS NOT NULL
GROUP BY l.zone_name, l.city, l.region
ORDER BY avg_delivery_min DESC;


-- Q3: Payment method breakdown
-- Business question: "What % of revenue is digital vs cash?"
SELECT
    p.payment_method,
    p.is_digital,
    COUNT(*) AS order_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM fact_orders f
JOIN dim_payment p ON f.payment_method_id = p.payment_method_id
GROUP BY p.payment_method, p.is_digital
ORDER BY total_revenue DESC;


-- Q4: Cancellation analysis
-- Business question: "Why are orders being cancelled?"
SELECT
    s.order_status,
    s.cancellation_reason,
    COUNT(*) AS order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM fact_orders f
JOIN dim_order_status s ON f.order_status_id = s.order_status_id
GROUP BY s.order_status, s.cancellation_reason
ORDER BY order_count DESC;


-- Q5: Top drivers by rating and volume
-- Business question: "Who are our best drivers?"
SELECT
    d.driver_name,
    d.driver_rating,
    COUNT(*) AS deliveries,
    ROUND(AVG(f.delivery_time_minutes), 1) AS avg_delivery_min,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip_received
FROM fact_orders f
JOIN dim_driver d ON f.driver_id = d.driver_id
WHERE f.order_status_id = 1
GROUP BY d.driver_name, d.driver_rating
HAVING COUNT(*) >= 3
ORDER BY d.driver_rating DESC, avg_tip_received DESC
LIMIT 10;


-- Q6: Revenue by cuisine and city (cross-dimension analysis)
-- Business question: "Which cuisines perform best in which cities?"
-- This query JOINs fact with TWO dimensions — the power of Star Schema
SELECT
    l.city,
    r.cuisine_type,
    COUNT(*) AS orders,
    ROUND(SUM(f.total_amount), 2) AS revenue,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact_orders f
JOIN dim_restaurant r ON f.restaurant_id = r.restaurant_id
JOIN dim_location l ON f.zone_id = l.zone_id AND l.is_current = true
WHERE f.order_status_id = 1
GROUP BY l.city, r.cuisine_type
ORDER BY l.city, revenue DESC;


-- Q7: Hourly order pattern
-- Business question: "When do most orders come in?"
SELECT
    HOUR(CAST(f.order_timestamp AS TIMESTAMP)) AS order_hour,
    COUNT(*) AS order_count,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    ROUND(AVG(f.delivery_time_minutes), 1) AS avg_delivery_min
FROM fact_orders f
GROUP BY HOUR(CAST(f.order_timestamp AS TIMESTAMP))
ORDER BY order_hour;


-- Q8: Discount impact on order value
-- Business question: "Do discounts increase order size?"
SELECT
    CASE
        WHEN f.discount = 0 THEN 'No discount'
        WHEN f.discount < 5 THEN 'Small (under $5)'
        WHEN f.discount < 10 THEN 'Medium ($5-$10)'
        ELSE 'Large ($10+)'
    END AS discount_category,
    COUNT(*) AS order_count,
    ROUND(AVG(f.food_cost), 2) AS avg_food_cost,
    ROUND(AVG(f.total_amount), 2) AS avg_total,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip
FROM fact_orders f
GROUP BY
    CASE
        WHEN f.discount = 0 THEN 'No discount'
        WHEN f.discount < 5 THEN 'Small (under $5)'
        WHEN f.discount < 10 THEN 'Medium ($5-$10)'
        ELSE 'Large ($10+)'
    END
ORDER BY avg_total DESC;
