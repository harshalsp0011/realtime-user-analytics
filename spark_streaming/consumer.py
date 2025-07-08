from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Define schema for incoming Kafka JSON
event_schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaUserEventStream") \
    .getOrCreate()

# Read stream from Kafka topic
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from binary to string and parse JSON
parsed_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), event_schema).alias("data")) \
    .select("data.*")

# Write to console (for now)
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
