"""
This file is to give an example for how to get data from kafka topic to spark df
"""
from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp
import os


# Read Schema from inventories_schema.json
with open('schemas/inventories_schema.json', 'r') as f:
    inventories_schema = f.read()


def get_inventories_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    inventories_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_INVENTORIES")) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract Values from cars_df
    inventories_df = inventories_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), inventories_schema).alias("inventories")) 
    return inventories_df
