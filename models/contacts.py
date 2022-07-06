from spark_obj import get_or_create
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp
import os


# Read Schema from contacts_schema.json
with open('schemas/contacts_schema.json', 'r') as f:
    cars_schema = f.read()


def get_contacts_df(app_name):
    # Read stream for cars
    spark = get_or_create(app_name=app_name)
    contacts_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get('BOOTSTRAP_SERVER')) \
        .option("subscribe", os.environ.get("TOPIC_CONTACTS")) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract Values from cars_df
    contacts_df = contacts_df.selectExpr("substring(value, 6) as value") \
        .select(from_avro(col("value"), cars_schema).alias("contacts")) 
    return contacts_df
