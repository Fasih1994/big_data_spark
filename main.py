from spark_obj import get_or_create, setLogLevel
from models.orders import get_orders_df
from models.cars import get_cars_df

APP_NAME = "SPARK STREAMING"

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
