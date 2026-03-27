from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================
# BRONZE LAYER — Event Hub Streaming + Bulk Load
# ============================================================
# Prerequisites:
#   Cluster Spark config must have:
#     spark.connection_string <Event Hub connection string>
#     spark.hadoop.fs.azure.account.key.<storage>.dfs.core.windows.net <key>
# ============================================================

STORAGE = "foodexpresslake"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"

# --- Event Hub / Kafka Configuration ---
EH_NAMESPACE = "foodexpress-std"
EH_NAME = "ordertopic"
EH_CONN_STR = spark.conf.get("spark.connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}

# --- Stream 1: Real-time orders from Event Hub ---
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

df_bronze = df_raw.withColumn("order_raw", col("value").cast("string"))

(
    df_bronze
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{BRONZE}/checkpoints/orders_raw")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .toTable("orders_raw")
)

# --- Stream 2: Bulk historical orders ---
df_bulk = spark.read.option("multiLine", True).json(f"{BRONZE}/bulk_orders.json")
df_bulk.write.format("delta").mode("overwrite").saveAsTable("bulk_orders")

# --- Load all mapping tables ---
mapping_files = [
    "map_restaurants", "map_cuisines", "map_payment_methods",
    "map_order_statuses", "map_delivery_zones", "map_cancellation_reasons",
]

for filename in mapping_files:
    df = spark.read.option("multiLine", True).json(f"{BRONZE}/{filename}.json")
    df.write.format("delta").mode("overwrite").saveAsTable(filename)
    print(f"Loaded {filename}: {df.count()} records")