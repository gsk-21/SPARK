from pyspark.sql.types import IntegerType, StringType, StructType, StructField

from utils import spark

num_list = [1, 2, 3, 4, 5]

print("Integers.....")

df = spark.createDataFrame(num_list, schema='int')
df.show()

df = spark.createDataFrame(num_list, schema=IntegerType())
df.show()

num_list = [(1,), (2,), (3,)]
df = spark.createDataFrame(num_list, schema='a int')
df.show()

print("String.....")

str_list = ['Sen', 'John', 'Rick']
df = spark.createDataFrame(str_list, schema='string')
df.show()

df = spark.createDataFrame(str_list, schema=StringType())
df.show()

print("Multiple Cols")

multiple_cols = [
    (1, 'Sen'),
    (2, 'John')
]
df = spark.createDataFrame(multiple_cols)
df.show()

df = spark.createDataFrame(multiple_cols, schema='ID int, Name string')
df.show()

df = spark.createDataFrame(multiple_cols, schema=StructType(
    [StructField('ID', IntegerType()),
     StructField('Name', StringType())]
))
df.show()
