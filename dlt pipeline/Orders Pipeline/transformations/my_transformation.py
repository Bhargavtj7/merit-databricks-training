import dlt
from pyspark.sql.functions import col
@dlt.table(name = "bronze_orders")

def load_orders():
    orders_df = spark.readStream.format("cloudfiles").option("cloudfiles.format","json").load("/Volumes/merit_catalog/quickstart_schema/sandbox/dataset/e-commerce/staging/Orders/")
    return orders_df

@dlt.table(name = "silver_orders")
def load_to_silver():
    df = dlt.read_stream("bronze_orders").filter(col("order_id").isNotNull())
    return df

@dlt.table(name = "gold_orders")
def load_to_gold():
    df = dlt.read_stream("silver_orders").groupBy("item_id").count()
    return df