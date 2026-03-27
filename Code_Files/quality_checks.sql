-- ============================================================
-- FOODEXPRESS — Data Quality Checks
-- Run after each pipeline execution to validate data integrity
-- ============================================================


-- CHECK 1: No null primary keys in fact table
-- If this returns rows, something is broken
SELECT 'FAIL: null order_id in fact_orders' AS check_result,
       COUNT(*) AS bad_rows
FROM fact_orders
WHERE order_id IS NULL
HAVING COUNT(*) > 0;


-- CHECK 2: No orphan foreign keys (fact → dim referential integrity)
-- Every restaurant_id in fact must exist in dim_restaurant
SELECT 'FAIL: orphan restaurant_id' AS check_result,
       COUNT(*) AS bad_rows
FROM fact_orders f
LEFT JOIN dim_restaurant r ON f.restaurant_id = r.restaurant_id
WHERE r.restaurant_id IS NULL
HAVING COUNT(*) > 0;


-- CHECK 3: No negative amounts
SELECT 'FAIL: negative total_amount' AS check_result,
       COUNT(*) AS bad_rows
FROM fact_orders
WHERE total_amount < 0
HAVING COUNT(*) > 0;


-- CHECK 4: Delivery time within reasonable range (1-180 minutes)
SELECT 'FAIL: unreasonable delivery_time' AS check_result,
       COUNT(*) AS bad_rows
FROM fact_orders
WHERE delivery_time_minutes < 1 OR delivery_time_minutes > 180
HAVING COUNT(*) > 0;


-- CHECK 5: Rating within valid range (1-5) or null
SELECT 'FAIL: invalid rating' AS check_result,
       COUNT(*) AS bad_rows
FROM fact_orders
WHERE rating IS NOT NULL AND (rating < 1 OR rating > 5)
HAVING COUNT(*) > 0;


-- CHECK 6: SCD Type 2 integrity — exactly one current row per zone
SELECT 'FAIL: multiple current rows in dim_location' AS check_result,
       zone_id,
       COUNT(*) AS current_count
FROM dim_location
WHERE is_current = true
GROUP BY zone_id
HAVING COUNT(*) > 1;


-- CHECK 7: Row count sanity — Silver should equal or exceed Bronze
SELECT
    'Bronze bulk_orders' AS layer,
    COUNT(*) AS row_count
FROM bulk_orders
UNION ALL
SELECT
    'Silver stg_orders' AS layer,
    COUNT(*) AS row_count
FROM stg_orders
UNION ALL
SELECT
    'Silver silver_obt' AS layer,
    COUNT(*) AS row_count
FROM silver_obt
UNION ALL
SELECT
    'Gold fact_orders' AS layer,
    COUNT(*) AS row_count
FROM fact_orders;


-- CHECK 8: No duplicate order_ids in fact table
SELECT 'FAIL: duplicate order_id' AS check_result,
       order_id,
       COUNT(*) AS duplicates
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1
LIMIT 5;


-- CHECK 9: All mapping tables have records
SELECT 'dim_customer' AS dim_table, COUNT(*) AS rows FROM dim_customer
UNION ALL
SELECT 'dim_driver', COUNT(*) FROM dim_driver
UNION ALL
SELECT 'dim_restaurant', COUNT(*) FROM dim_restaurant
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM dim_payment
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_order_status', COUNT(*) FROM dim_order_status;
