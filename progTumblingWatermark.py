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
    schema = 'order_id long, order_date timestamp, order_customer_id long, order_status string, amount long'
    orders_df = spark.readStream \
                 .format("socket") \
                 .option("host", "localhost") \
                 .option("port", 9972) \
                 .load()

    # 2. processing logic
    df_json = orders_df.select(from_json(col('value'), schema).alias('value'))
    cleaned_df = df_json.select("value.*")

    window_agg_df = cleaned_df \
                    .withWatermark("order_date", "30 minutes") \
                    .groupBy(window("order_date", "10 minutes")) \
                    .agg(sum("amount").alias("totalInvoice"))

    output_df = window_agg_df.select("window.start", "window.end", "totalInvoice")

    # output_df.printSchema()

    # 3. write to the sink
    query = output_df.writeStream \
                     .outputMode("update") \
                     .format("console") \
                     .option("checkpointLocation", "checkpointdir01") \
                     .trigger(processingTime = "15 second") \
                     .start()
    
    query.awaitTermination()