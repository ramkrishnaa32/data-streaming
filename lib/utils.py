from pyspark.sql import SparkSession
import getpass

def initiate_spark_session(appName):
    username = getpass.getuser()
    spark = SparkSession. \
        builder. \
        appName(appName). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        master('local'). \
        getOrCreate()
    return spark