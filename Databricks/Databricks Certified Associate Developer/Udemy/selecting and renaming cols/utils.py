from pyspark.sql import SparkSession
import datetime
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, Row


try:
    spark = SparkSession.builder.master("local").appName("Databricks").getOrCreate()
    print("\nSetting log Level to ERROR\n")
    spark.sparkContext.setLogLevel("ERROR")
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
            "courses": [1]
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

except Exception as error:
    raise error





