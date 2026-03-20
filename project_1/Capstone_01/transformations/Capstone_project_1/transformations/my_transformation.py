import dlt
from pyspark.sql.functions import col,sum,lit,when,concat,regexp_replace,lower
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




@dlt.table(name="silver_ecommerce")

def silver_ecommerce():

    orders_df = dlt.read("bronze_orders")

    customers_df = dlt.read("bronze_customers")

    products_df = dlt.read("bronze_products")

    result_df = (

        orders_df.alias("o")

        .join(

            customers_df.alias("c"),

            col("o.customer_id") == col("c.customer_id"),

            "inner",

        )

        .join(products_df.alias("p"), col("o.item_id") == col("p.product_id"), "inner")

    )

    result_df = result_df.na.drop(subset=["o.customer_id"])

    
    result_df = result_df.withColumn("c.name", when(col("c.name").isNull(), "Anonymous").otherwise(col("c.name")))

    result_df = result_df.withColumn("c.email",when(col("c.email").isNull(),concat(lower(regexp_replace(col("c.name"), " ", ".")),lit("@gmail.com"))).otherwise(col("c.email")))



    result_df = result_df.select(

        col("o.order_id"),

        col("o.customer_id"),
        col("p.product_id"),
        col("o.qty"),

        col("c.name").alias("customer_name"),

        col("p.name").alias("product_name"),

        

        col("o.price"),

    )

    result_df = result_df.withColumn("total_amount", col("o.qty") * col("o.price"))

    result_df = result_df.filter(col("o.order_id").isNotNull())

    return result_df

@dlt.table(name="Gold_ecommerce_1")

def Gold_ecommerce_1():


    silver_df = dlt.read("silver_ecommerce")

    gold_df = silver_df.groupBy("product_id").agg(sum(col("qty").cast("int")).alias("total_count_p"))

    return gold_df

@dlt.table(name="Gold_ecommerce_2")

def Gold_ecommerce_2():


    silver_df = dlt.read("silver_ecommerce")
    golds_df = silver_df.groupBy("customer_id").agg(sum(col("qty").cast("int")).alias("total_count_c"))

    return golds_df



    

    
 
 