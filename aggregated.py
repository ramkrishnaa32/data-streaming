from lib.utils import initiate_spark_session

spark = initiate_spark_session('Orders Aggregated')

schema = """order_id long, customer_id long, customer_fname string, customer_lname string, city string, state string, pincode long,
            line_iems array<struct<order_item_id: long, order_item_product_id: long, order_item_quantity: long, order_item_product_price: float,
            order_item_subtotal: float>>"""

ordersAgg = spark.read \
                 .format("json") \
                 .schema(schema) \
                 .load("data/aggregated/")

ordersAgg = ordersAgg.withColumnRenamed("line_iems", "line_items")

ordersAgg.createOrReplaceTempView("ordersAgg")

exploded_orders = spark.sql("""select order_id, customer_id, city,state,
                               pincode, explode(line_items) lines from ordersAgg""")

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

spark.sql("select * from orders_aggregated where customer_id = 256").show()

