from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import os



# Read Schema from cars_schema.json
with open('schemas/cars_schema.json', 'r') as f:
    cars_schema = f.read()


def get_cars_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    cars_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_CARS")) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Extract Values from cars_df
    cars_df = cars_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), cars_schema).alias("cars")) \
        .select("cars.ID", "cars.NAME", "cars.YEAR", "cars.PRICE")
    return cars_df