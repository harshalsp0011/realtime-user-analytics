from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from postgres_writer.db_writer import write_trends_to_postgres


# Define schema
event_schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())  # ISO string

# Start session
spark = SparkSession.builder \
    .appName("EventsPerMinute") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and convert timestamp to real timestamp type
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), event_schema).alias("data")) \
    .select(
        col("data.user_id"),
        col("data.product_id"),
        col("data.event_type"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    )

# Aggregate: Count events per 1-minute window and event type
# windowed_counts = parsed_df \
#     .groupBy(
#         window(col("event_time"), "1 minute"),
#         col("event_type")
#     ).count() \
#     .orderBy("window")




windowed_counts = parsed_df \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("event_type")
    ).count() \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window") \
    .orderBy("window_start")


# Output to console
# query = windowed_counts.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime="30 seconds") \
#     .start()

# ooutput save to db .
query = windowed_counts.writeStream \
    .foreachBatch(write_trends_to_postgres) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
