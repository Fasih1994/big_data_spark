from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import os


# Read Schema from employees_schema.json
with open('schemas/employees_schema.json', 'r') as f:
    employees_schema = f.read()


def get_employees_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    employees_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_EMPLOYEES")) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract Values from cars_df
    employees_df = employees_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), employees_schema).alias("employees"))
        
    return employees_df
