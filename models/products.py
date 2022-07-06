"""
This file is to give an example for how to get data from kafka topic to spark df
"""
from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import os


# Read Schema from cars_schema.json
with open('schemas/products_schema.json', 'r') as f:
    schema = f.read()


def get_products_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_PRODUCTS")) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract Values from cars_df
    df = df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), schema).alias("products"))
    return df
