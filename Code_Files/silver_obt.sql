-- ============================================================
-- SILVER OBT — One Big Table (JOIN stg_orders with 6 mappings)
-- ============================================================

CREATE OR REPLACE TABLE silver_obt AS

SELECT
    o.order_id, o.customer_id, o.driver_id,
    o.restaurant_id, o.cuisine_id, o.zone_id,
    o.payment_method_id, o.order_status_id, o.cancellation_reason_id,
    o.customer_name, o.customer_email, o.customer_phone, o.delivery_address,
    o.driver_name, o.driver_rating, o.driver_phone,
    o.delivery_latitude, o.delivery_longitude,
    o.order_timestamp, o.pickup_timestamp, o.delivery_timestamp,
    o.item_count, o.food_cost, o.delivery_fee, o.tax_amount,
    o.discount, o.tip_amount, o.total_amount,
    o.prep_time_minutes, o.delivery_time_minutes, o.rating,

    r.restaurant_name, r.avg_prep_time_min,
    c.cuisine_type,
    pm.payment_method, pm.is_digital,
    os.order_status, os.is_completed,
    dz.zone_name, dz.city, dz.state, dz.region,
    cr.cancellation_reason

FROM stg_orders o
    LEFT JOIN map_restaurants r        ON o.restaurant_id = r.restaurant_id
    LEFT JOIN map_cuisines c           ON o.cuisine_id = c.cuisine_id
    LEFT JOIN map_payment_methods pm   ON o.payment_method_id = pm.payment_method_id
    LEFT JOIN map_order_statuses os    ON o.order_status_id = os.order_status_id
    LEFT JOIN map_delivery_zones dz    ON o.zone_id = dz.zone_id
    LEFT JOIN map_cancellation_reasons cr ON o.cancellation_reason_id = cr.cancellation_reason_id