from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================
# SILVER LAYER — Parse JSON + Merge Streams into stg_orders
# ============================================================

STORAGE = "foodexpresslake"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"

# Schema for parsing Event Hub JSON orders
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("restaurant_id", LongType(), True),
    StructField("cuisine_id", LongType(), True),
    StructField("zone_id", LongType(), True),
    StructField("payment_method_id", LongType(), True),
    StructField("order_status_id", LongType(), True),
    StructField("cancellation_reason_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("delivery_address", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_rating", DoubleType(), True),
    StructField("driver_phone", StringType(), True),
    StructField("delivery_latitude", DoubleType(), True),
    StructField("delivery_longitude", DoubleType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("pickup_timestamp", StringType(), True),
    StructField("delivery_timestamp", StringType(), True),
    StructField("item_count", LongType(), True),
    StructField("food_cost", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("prep_time_minutes", LongType(), True),
    StructField("delivery_time_minutes", LongType(), True),
    StructField("rating", DoubleType(), True),
])

# --- Parse Event Hub stream ---
df_stream_parsed = (
    spark.readStream
    .table("orders_raw")
    .withColumn("parsed", from_json(col("order_raw"), order_schema))
    .select("parsed.*")
)

# --- Read bulk orders as stream ---
df_bulk_stream = spark.readStream.table("bulk_orders")

# --- Merge both into stg_orders ---
df_stg_orders = df_stream_parsed.unionByName(df_bulk_stream)

(
    df_stg_orders
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{SILVER}/checkpoints/stg_orders")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .toTable("stg_orders")
)