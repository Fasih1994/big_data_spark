"""
This file is to give an example for how to get data from kafka topic to spark df
"""
from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp
import os


# Read Schema from countries_schema.json
with open('schemas/countries_schema.json', 'r') as f:
    contries_schema = f.read()


def get_countries_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    countries_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_COUNTRIES")) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract Values from cars_df
    countries_df = countries_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), contries_schema).alias("countries")) \
        .select(
            "cars.ID",
            col("cars.NAME").alias("CAR_NAME"),
            "cars.YEAR",
            "cars.PRICE"
        ).withColumn("purchase_time", current_timestamp())
    return countries_df
