import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

try:
    print("Reading excel...")
    path = "C:\\Users\\$en\\Documents\\GitHub\\SPARK\\Databricks\\Data\\web_sales_excel.xlsx"
    df = pd.read_excel(path, engine='openpyxl')
    print("Read completed")
    print(df)

    print("Creating Spark object")
    conf = SparkConf().setAppName("Databricks")
    sc = SparkContext(conf=conf)
    jars_path = "C:\\Users\\$en\\Documents\\GitHub\\SPARK\\jar_files"
    spark = SparkSession.builder.appName("Databricks").getOrCreate()
    sc.setLogLevel("ERROR")
    new_df = spark.createDataFrame(df)
    new_df.show()
except Exception as e:
    print("Error occurred")
    raise e
