from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder.master("local").appName("Databricks").getOrCreate()
    print("\nSetting log Level to ERROR\n")
    spark.sparkContext.setLogLevel("ERROR")
except Exception as error:
    raise error
