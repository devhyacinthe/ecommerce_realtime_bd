import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import clickhouse_connect

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/bronze/events")
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("country", StringType()),
    StructField("payment_method", StringType()),
])

spark = (SparkSession.builder
         .appName("EcommerceRealtime")
         .config("spark.sql.shuffle.partitions", "6")   # = nb de cœurs
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

events = (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", to_timestamp(col("event_time"))))

# --- BRONZE : événements bruts en Parquet ---
bronze_query = (events.writeStream
                .format("parquet")
                .option("path", BRONZE_PATH)
                .option("checkpointLocation", f"{CHECKPOINT}/bronze")
                .partitionBy("category")
                .outputMode("append")
                .trigger(processingTime="30 seconds")
                .start())

# --- GOLD : CA par minute, par catégorie et pays ---
agg = (events.filter(col("event_type") == "purchase")
       .withWatermark("event_time", "2 minutes")
       .groupBy(window(col("event_time"), "1 minute"), col("category"), col("country"))
       .agg(count("*").alias("orders"),
            _sum(col("price") * col("quantity")).alias("revenue"),
            _sum("quantity").alias("units"))
       .select(col("window.start").alias("window_start"),
               col("window.end").alias("window_end"),
               "category", "country", "orders", "revenue", "units"))

def write_to_clickhouse(batch_df, batch_id):
    rows = batch_df.collect()           # agrégats = peu de lignes, sûr à collecter
    if not rows:
        return
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DB)
    data = [[r["window_start"], r["window_end"], r["category"], r["country"],
             r["orders"], float(r["revenue"]), r["units"]] for r in rows]
    client.insert("sales_by_minute", data,
                  column_names=["window_start", "window_end", "category",
                                "country", "orders", "revenue", "units"])
    client.close()

gold_query = (agg.writeStream
              .foreachBatch(write_to_clickhouse)
              .option("checkpointLocation", f"{CHECKPOINT}/gold")
              .outputMode("append")    # n'émet une fenêtre qu'une fois finalisée
              .trigger(processingTime="30 seconds")
              .start())

spark.streams.awaitAnyTermination()