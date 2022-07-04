from dotenv import load_dotenv
from spark_obj import get_or_create, setLogLevel
from models.orders import get_orders_df
from models.cars import get_cars_df
import os


load_dotenv(dotenv_path='dev.env')
APP_NAME = os.environ["APPLICATION_NAME"]

# Get SPARK Object
spark = get_or_create(app_name=APP_NAME)
setLogLevel(app_name=APP_NAME, level='WARN')


cars_df = get_cars_df(app_name=APP_NAME)
orders_df = get_orders_df(app_name=APP_NAME)

query = orders_df\
    .writeStream \
    .format("console") \
    .outputMode('append') \
    .start()

query.awaitTermination()
