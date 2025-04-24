from pyspark.sql.functions import *
from pyspark.sql import SparkSession


if __name__ == '__main__':
    
    print(f"Intiating sparkSession")
    spark = SparkSession.builder \
                        .appName("Structured Streaming") \
                        .config("spark.sql.shuffle.partitions", 3) \
                        .master("local[2]") \
                        .getOrCreate()
    
    # spark.sparkContext.setLogLevel("INFO")

     # 1. read the data
    schema = """order_id long,customer_id long, customer_fname string, customer_lname string, city string, state string, pincode long, 
                       line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"""
    
    orders_df = spark.readStream \
                 .format("json") \
                 .schema(schema) \
                 .option("path", "data/aggregated/input") \
                 .load()

    # 2. processing logic
    orders_df.createOrReplaceTempView("orders")

    exploded_orders = spark.sql("""select order_id, customer_id, city,state,
                               pincode, explode(line_items) lines from orders""")
    
    exploded_orders.createOrReplaceTempView("exploded_orders")

    flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
                                lines.order_item_id as item_id, lines.order_item_product_id as product_id,
                                lines.order_item_quantity as quantity,lines.order_item_product_price as price,
                                lines.order_item_subtotal as subtotal from exploded_orders""")

    flattened_orders.createOrReplaceTempView("orders_flattened")

    aggregated_orders = spark.sql("""select customer_id, count(distinct(order_id)) as orders_placed, 
                                 count(item_id) as products_purchased,sum(subtotal) as amount_spent 
                                 from orders_flattened group by customer_id""")

    aggregated_orders.createOrReplaceTempView("orders_aggregated")

    
    # 3. write to the sink
    query = aggregated_orders.writeStream \
                 .format("json") \
                 .outputMode("complete") \
                 .option("path", "data/aggregated/") \
                 .option("checkpointLocation", "checkpointdir01") \
                 .start()
    
    query.awaitTermination()

