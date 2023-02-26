import os
import platform
from pyspark.sql import SparkSession

# from pyspark.sql.types import *

os.environ["JAVA_HOME"] = "/home/sen-21/java/jre"
current_platform = platform.system().lower()

if current_platform == 'linux':
    data_directory = "/home/sen-21/Documents/GitHub/SPARK/Databricks/data/"
else:
    data_directory = "C:\\Users\\$en\\Documents\\GitHub\\SPARK\\Databricks\\Data\\"

try:
    print(f"Platform:{current_platform}\nData Directory:{data_directory}")
    json_file = data_directory + "customer.json"
    parquet_file = data_directory + "address.parquet"
    csv_file = data_directory + "web_sales.csv"
    dat_file = data_directory + "item.dat"

    data = [
        ("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
    ]
    structure_data = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]

    print("\nCreating Spark object")
    spark = SparkSession.builder.master("local").appName("Databricks").getOrCreate()
    print("\nSpark object created successfully")
    print("\nSpark object:", spark)
except Exception as error:
    raise error

# import findspark
# findspark.init()
# config("spark.ui.port", 5050)
# spark = SparkSession.builder.appName("Databricks").getOrCreate()
# conf = SparkConf().setAppName("Databricks")
# sc = SparkContext(conf=conf)
# jars_path = "C:\\Users\\$en\\Documents\\GitHub\\SPARK\\jar_files"
# conf.set("spark.jars", jars_path + "spark-excel_2.11-0.12.2.jar")
# sc.setLogLevel("ERROR")
# df = spark.read.format('json').load(path + "customer.json")
# df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "|").csv(dat_file)
# df = spark.read.format('csv').load(csv_file)
