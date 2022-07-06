from unittest import result
from dotenv import load_dotenv
from spark_obj import get_or_create, setLogLevel
from models.orders import get_orders_df
import os


load_dotenv(dotenv_path='dev.env')
APP_NAME = os.environ["APPLICATION_NAME"]

# Get SPARK Object
spark = get_or_create(app_name=APP_NAME)
setLogLevel(app_name=APP_NAME, level='WARN')



query = result\
    .writeStream \
    .format("console") \
    .outputMode('complete')\
    .start()

query.awaitTermination()






