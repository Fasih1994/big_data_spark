# Read libraries
from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp
import os


# Read Schema
with open('schemas/orders_schema.json', 'r') as f:
    orders_schema = f.read()


def get_orders_df(app_name):
    spark = get_or_create(app_name=app_name)
    # Read stream from kafka topic for ORDERS
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("BOOTSTRAP_SERVER")) \
        .option("subscribe", os.environ.get("TOPIC_ORDERS")) \
        .option("startingOffsets", "earliest") \
        .load()

    orders_df = orders_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), orders_schema).alias("orders"))

    return orders_df
