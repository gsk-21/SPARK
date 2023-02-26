import datetime

from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from utils import spark

users = [
    {
        "id": 1,
        "first_name": "Senthil",
        "last_name": "Kumar",
        "email": "gsk6555@gmail.com",
        "is_customer": True,
        "amount_paid": 1000.15,
        "customer_from": datetime.date(2021, 1, 5),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0),
        "phone_number": ['8072786314', '9677381546']
    },
    {
        "id": 2,
        "first_name": "John",
        "last_name": "Cena",
        "email": "jc@gmail.com",
        "is_customer": True,
        "amount_paid": 500.00,
        "customer_from": datetime.date(2021, 5, 5),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 25, 10),
        "phone_number": ['9677568748']
    },
    {
        "id": 3,
        "first_name": "Brock",
        "last_name": "Lesnar",
        "email": "brock@gmail.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2023, 2, 10, 1, 15),
        "phone_number": []
    },
]

df_list = [Row(**user) for user in users]
print(df_list)
df = spark.createDataFrame(df_list)
df.printSchema()
print("\nDF Columns: ", df.columns)
print("\nData Types: ", df.dtypes)
df.show()

df.select(
    'id',
    col('phone_number')[0].alias('Mobile'),
    col('phone_number')[1].alias('Phone')
).show()

print("\nExplode..............")
df.select(
    'id',
    explode('phone_number')
).show()

print("\nExplode Outer..............")
df.select(
    'id',
    explode_outer('phone_number')
).show()
