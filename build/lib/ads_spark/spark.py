from pyspark.sql import SparkSession

def get_spark():
    return (SparkSession.builder
                .master("local")
                .appName("ads_spark")
                .getOrCreate())