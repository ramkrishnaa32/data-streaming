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
    schema = "order_id long, order_date date, order_customer_id long, order_status string"
    orders_df = spark.readStream \
                 .format("json") \
                 .schema(schema) \
                 .option("path", "data/inputdir") \
                 .load()

    # 2. processing logic
    orders_df.createOrReplaceTempView("orders")
    aggregsted_orders = spark.sql("select order_status, count(*) as totalCount from orders group by order_status")

    # 3. write to the sink
    query = aggregsted_orders.writeStream \
                 .format("console") \
                 .outputMode("complete") \
                 .option("checkpointLocation", "checkpointdir01") \
                 .start()
    
    query.awaitTermination()

