from pyspark.sql import Row
from utils import spark

row = Row(1, 'Sen')
print("Row 1: ", row)

row = Row(id=2, Name='Sen')
print("Row 2: ", row)

data_list = [
    [1, 'Sen'],
    [2, 'John']
]

df_list = [Row(*i) for i in data_list]
print(df_list)
df = spark.createDataFrame(df_list)
df.show()

data_list = [
    (3, 'Sen'),
    [4, 'John']
]

df_list = [Row(*i) for i in data_list]
print(df_list)
df = spark.createDataFrame(df_list)
df.show()

data_list = [
    {'ID': 5, 'Name': 'Sen'},
    {'ID': 6, 'Name': 'Brock'}
]

df_list = [Row(*i.values()) for i in data_list]
print(df_list)
df = spark.createDataFrame(df_list)
df.show()


data_list = [
    {'ID': 7, 'Name': 'Sen'},
    {'ID': 8, 'Name': 'Brock'}
]

df_list = [Row(**i) for i in data_list]
print(df_list)
df = spark.createDataFrame(df_list)
df.show()
