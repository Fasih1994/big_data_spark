from pyspark.sql import SparkSession


def get_or_create(app_name=None):
    spark = SparkSession \
        .builder \
        .appName("STREAM" if not app_name else app_name) \
        .getOrCreate()

    print(f"Created SPARK session with name = {app_name}")
    return spark


def setLogLevel(app_name, level=None):
    spark = get_or_create(app_name)
    if level:
        print(f"Setting spark log level to {level}")
        spark.sparkContext.setLogLevel(level)
    

