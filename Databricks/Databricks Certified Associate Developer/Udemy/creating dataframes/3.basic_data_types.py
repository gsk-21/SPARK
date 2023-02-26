import datetime

from pyspark.sql import Row
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
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "John",
        "last_name": "Cena",
        "email": "jc@gmail.com",
        "is_customer": True,
        "amount_paid": 500.00,
        "customer_from": datetime.date(2021, 5, 5),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 25, 10)
    },
    {
        "id": 3,
        "first_name": "Brock",
        "last_name": "Lesnar",
        "email": "brock@gmail.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2023, 2, 10, 1, 15)
    },
]

df_list = [Row(**user) for user in users]
print(df_list)
df = spark.createDataFrame(df_list)
df.printSchema()
print("\nDF Columns: ", df.columns)
print("\nData Types: ", df.dtypes)
df.show()

users.append(
    {
        "id": 4,
        "first_name": "Randy",
        "last_name": "Ortan",
        "email": "randy@gmail.com",
        "is_customer": False,
        "amount_paid": 250.20,
        "customer_from": datetime.date(2023, 1, 5),
        "last_updated_ts": datetime.datetime(2023, 2, 10, 1, 15)
    },
)

print("\nUsers:")
print(users)

schema = '''
id int,
first_name string,
last_name string,
email string,
is_customer boolean,
amount_paid float,
customer_from date,
last_updated_ts timestamp
'''

df = spark.createDataFrame(data=users, schema=schema)
df.printSchema()
print("\nDF Columns: ", df.columns)
print("\nData Types: ", df.dtypes)
df.show()

print("\nColumn Names with list")

users = [
    (
        1, "Senthil", "Kumar", "gsk6555@gmail.com", True, 1000.15, datetime.date(2021, 1, 5),
        datetime.datetime(2021, 2, 10, 1, 15, 0)
    ),
    (
        2, "Chris", "Bumstead", "cbum@gmail.com", False, 100.15, datetime.date(2021, 1, 5),
        datetime.datetime(2021, 2, 10, 1, 15, 0)
    ),

]

schema_list = [
    'ID',
    'FIRST_NAME',
    'LAST_NAME',
    'EMAIL',
    'IS_CUSTOMER',
    'AMOUNT_PAID',
    'CUSTOMER_FROM',
    'LAST_UPDATED_TS'
]

df = spark.createDataFrame(data=users, schema=schema_list)

df.printSchema()
print("\nDF Columns: ", df.columns)
print("\nData Types: ", df.dtypes)
df.show()

print("\nColumn Names using Spark Datatypes")

users = [
    (
        1, "Senthil", "Kumar", "gsk6555@gmail.com", True, 1000.15, datetime.date(2021, 1, 5),
        datetime.datetime(2021, 2, 10, 1, 15, 0)
    ),
    (
        2, "Chris", "Bumstead", "cbum@gmail.com", False, 100.15, datetime.date(2021, 1, 5),
        datetime.datetime(2021, 2, 10, 1, 15, 0)
    ),

]

schema_list = StructType(
    [
        StructField(name='ID', dataType=IntegerType()),
        StructField(name='FIRST_NAME', dataType=StringType()),
        StructField(name='LAST_NAME', dataType=StringType()),
        StructField(name='EMAIL', dataType=StringType()),
        StructField(name='IS_CUSTOMER', dataType=BooleanType()),
        StructField(name='AMOUNT_PAID', dataType=FloatType()),
        StructField(name='CUSTOMER_FROM', dataType=DateType()),
        StructField(name='LAST_UPDATED_TS', dataType=TimestampType())
    ]
)
df = spark.createDataFrame(data=users, schema=schema_list)

df.printSchema()
print("\nDF Columns: ", df.columns)
print("\nData Types: ", df.dtypes)
df.show()
