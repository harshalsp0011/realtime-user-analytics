from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, countDistinct,approx_count_distinct,current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from postgres_writer.db_writer import write_funnel_to_postgres


# Define schema for parsing events
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Start Spark
spark = SparkSession.builder.appName("FunnelAnalytics").getOrCreate()

# Read Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON + convert timestamp
events_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select(
        col("data.user_id"),
        col("data.event_type"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    )

# Aggregate: Distinct users per event_type in each 1-minute window
# funnel_df = events_df.groupBy(
#     window(col("event_time"), "1 minute"),
#     col("event_type")
# ).agg(
#     approx_count_distinct("user_id").alias("unique_users")
# ).orderBy("window")


funnel_df = events_df.groupBy(
    window(col("event_time"), "1 minute"),
    col("event_type")
).agg(
    approx_count_distinct("user_id").alias("unique_users")
).withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .withColumn("batch_time", current_timestamp()) \
 .select("window_start", "window_end", "event_type", "unique_users", "batch_time")

# Output. on console
# query = funnel_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime="30 seconds") \
#     .start()

# store in writer.py/db
query = funnel_df.writeStream \
    .foreachBatch(write_funnel_to_postgres) \
    .outputMode("complete") \
    .start()


query.awaitTermination()
