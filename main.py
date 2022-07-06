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


# cars_df = get_cars_df(app_name=APP_NAME)

query = result\
    .writeStream \
    .format("console") \
    .outputMode('complete')\
    .start()

query.awaitTermination()









# orders_df.groupBy(
#         window('sale_time', '30 minutes'),
#         "CAR_NAME")
#     .count()