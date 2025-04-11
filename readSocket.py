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
    lines = spark.readStream \
                 .format("socket") \
                 .option("host", "localhost") \
                 .option("port", 9988) \
                 .load()

    # 2. processing logic
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    wordCounts = words.groupBy("word").count()

    # 3. write to the sink
    query = wordCounts.writeStream \
                 .outputMode("complete") \
                 .format("console") \
                 .option("checkpointLocation", "checkpointdir01") \
                 .start()
    
    query.awaitTermination()

