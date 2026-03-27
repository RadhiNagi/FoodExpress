from pyspark.sql.functions import *
from delta.tables import DeltaTable

# ============================================================
# GOLD LAYER — Star Schema (Fact + 6 Dimensions)
# ============================================================

# --- dim_customer (SCD Type 1) ---
(
    spark.table("silver_obt")
    .select("customer_id", "customer_name", "customer_email", "customer_phone")
    .dropDuplicates(["customer_id"])
    .write.format("delta").mode("overwrite").saveAsTable("dim_customer")
)

# --- dim_driver (SCD Type 1) ---
(
    spark.table("silver_obt")
    .select("driver_id", "driver_name", "driver_rating", "driver_phone")
    .dropDuplicates(["driver_id"])
    .write.format("delta").mode("overwrite").saveAsTable("dim_driver")
)

# --- dim_restaurant (SCD Type 1) ---
(
    spark.table("silver_obt")
    .select("restaurant_id", "restaurant_name", "cuisine_id", "cuisine_type", "avg_prep_time_min")
    .dropDuplicates(["restaurant_id"])
    .write.format("delta").mode("overwrite").saveAsTable("dim_restaurant")
)

# --- dim_payment (SCD Type 1) ---
(
    spark.table("silver_obt")
    .select("payment_method_id", "payment_method", "is_digital")
    .dropDuplicates(["payment_method_id"])
    .write.format("delta").mode("overwrite").saveAsTable("dim_payment")
)

# --- dim_order_status (SCD Type 1) ---
(
    spark.table("silver_obt")
    .select("order_status_id", "order_status", "is_completed", "cancellation_reason")
    .dropDuplicates(["order_status_id", "cancellation_reason"])
    .write.format("delta").mode("overwrite").saveAsTable("dim_order_status")
)

# --- dim_location (SCD Type 2) ---
df_new_locations = (
    spark.table("silver_obt")
    .select("zone_id", "zone_name", "city", "state", "region")
    .dropDuplicates(["zone_id"])
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

if not spark.catalog.tableExists("dim_location"):
    df_new_locations.write.format("delta").mode("overwrite").saveAsTable("dim_location")
else:
    dim_location = DeltaTable.forName(spark, "dim_location")
    dim_location.alias("old").merge(
        df_new_locations.alias("new"),
        "old.zone_id = new.zone_id AND old.is_current = true"
    ).whenMatchedUpdate(
        condition="old.region != new.region OR old.zone_name != new.zone_name",
        set={
            "effective_to": "current_timestamp()",
            "is_current": "false",
        }
    ).whenNotMatchedInsertAll(
    ).execute()

# --- fact_orders ---
(
    spark.table("silver_obt")
    .select(
        "order_id", "customer_id", "driver_id", "restaurant_id",
        "payment_method_id", "order_status_id", "zone_id",
        "order_timestamp", "pickup_timestamp", "delivery_timestamp",
        "item_count", "food_cost", "delivery_fee", "tax_amount",
        "discount", "tip_amount", "total_amount",
        "prep_time_minutes", "delivery_time_minutes", "rating",
    )
    .write.format("delta").mode("overwrite").saveAsTable("fact_orders")
)
