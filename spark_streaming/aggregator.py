from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc
from pyspark.sql.types import StructType, StringType, IntegerType
from postgres_writer.db_writer import write_aggregates_to_postgres

# Define event schema
event_schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("TopProductsAggregator") \
    .getOrCreate()

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Extract and parse the Kafka JSON messages
events_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), event_schema).alias("data")) \
    .select("data.*")

# Group by product_id and event_type, then count
aggregated_df = events_df.groupBy("product_id", "event_type") \
    .agg(count("*").alias("event_count")) \
    .orderBy(desc("event_count"))

# Write to console in real time. uncoment for testing on console
# query = aggregated_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime='10 seconds') \
#     .start()


query = aggregated_df.writeStream \
    .foreachBatch(write_aggregates_to_postgres) \
    .outputMode("complete") \
    .start()



query.awaitTermination()

