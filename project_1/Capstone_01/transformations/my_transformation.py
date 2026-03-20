import dlt
from pyspark.sql.functions import col
@dlt.table(name = "bronze_orders")
def load_orders():
    orders_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/merit_catalog/quickstart_schema/sandbox/dataset/e-commerce/staging/Orders/")
    return orders_df
@dlt.table(name = "bronze_products")
def load_products():
    products_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/merit_catalog/quickstart_schema/sandbox/dataset/e-commerce/staging/products/")
    return products_df
@dlt.table(name = "bronze_customers")
def load_customers():
    customer_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/merit_catalog/quickstart_schema/sandbox/dataset/e-commerce/staging/customers/")
    return customer_df




# @dlt.table("silver_orders")

# #@dlt.expect_or_drop("has_order_id", "order_id IS NOT NULL")
# def load_to_silver():

#     df = dlt.read_stream("bronze_orders").filter(col("order_id").isNotNull())
#     df.withcolumn("total_price", col("price") * col("quantity"))
#     return df

# @dlt.table("gold_orders")
# def load_to_gold():
#     df = dlt.read_stream("silver_orders").groupBy("item_id").count()
#     return df
