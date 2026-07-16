import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, from_json, to_date, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# --- Glue Init ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Kafka Configuration ---
KAFKA_BROKERS = "pkc-n98pk.us-west-2.aws.confluent.cloud:9092"
TOPIC         = "orders_topic"
KAFKA_API_KEY = "GIITD77LNNPTLUPY"
KAFKA_API_SECRET = "cfltGr8vg7/QUyxTssw3J2azvM7LLVn7DuZ/lVr+XscdG5uY/X9mEp3g6v6fG80g"

# --- Define your JSON schema ---
order_schema = StructType([
    StructField("order_id",      StringType(),  True),
    StructField("customer_id",   StringType(),  True),
    StructField("product",       StringType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("price",         DoubleType(),  True),
    StructField("order_timestamp", TimestampType(), True)
])

# --- Read from Kafka ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";') \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .load()

# --- Parse & clean ---
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

# Add helper columns for partitioning / auditing
df_clean = df_parsed \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("order_date", to_date(col("order_timestamp")))

S3_OUTPUT     = "s3://orders-realtime-bucket/orders2/"
CHECKPOINT_S3 = "s3://orders-realtime-bucket/checkpoints/orders2/"

query_s3 = df_clean.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", S3_OUTPUT) \
    .option("checkpointLocation", CHECKPOINT_S3) \
    .partitionBy("order_date") \
    .trigger(processingTime="30 seconds") \
    .start()

query_s3.awaitTermination()