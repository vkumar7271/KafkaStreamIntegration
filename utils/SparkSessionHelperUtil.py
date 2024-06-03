from pyspark.sql import SparkSession


def get_spark_session(app_name):
    try:
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", "2")
        return spark
    except Exception as err:
        print(err)
        exit(1)