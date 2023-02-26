import datetime
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, Row
from pyspark.sql.functions import *
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
        "phone_numbers": Row(phone='9867745618', home='8072786314'),
        "courses": [1, 2]
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
        "phone_numbers": Row(phone='8072786314', home='8697548639'),
        "courses": [1, 2]
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
        "phone_numbers": Row(phone=None, home=None),
        "courses": [1, 2]
    },
]

users_df = spark.createDataFrame(pd.DataFrame(users))

users_df.show()

print("\nSelect cols using *\n")

users_df.select('*').show()

print("\nSelect cols using column names\n")

users_df.select('id', 'first_name', 'last_name').show()

print("\nSelect cols using column names as list\n")

users_df.select(['id', 'first_name', 'last_name']).show()

print("\nSelect cols using alias\n")

users_df.alias('u').select('u.*').show()

users_df.select(
    'id',
    'first_name',
    'last_name',
    concat(col('first_name'),lit(' '), col('last_name')).alias('full name')
).show()
