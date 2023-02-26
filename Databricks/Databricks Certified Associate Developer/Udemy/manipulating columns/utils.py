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

    l = [('X',)]
    dummy_df = spark.createDataFrame(data=l, schema="dummy STRING")

    employee_details = [
        (1, 'Senthil', 'Kumar', 1000.0, 'India', '+91 807 278 6328', '456 87 9985'),
        (2, 'John', 'Cena', 800.0, 'United States', '+44 807 278 8956', '856 87 6955'),
        (3, 'Brock', 'Lesnar', 1200.0, 'United States', '+1 968 278 7368', '896 87 3678'),
        (4, 'Chris', 'Bumsted', 8900.0, 'Canada', '+61 458 295 3586', '689 35 4586'),
    ]

    employee_df = spark.createDataFrame(
        data=employee_details,
        schema="""
        id INT,
        first_name STRING,
        last_name STRING,
        salary float,
        nationality STRING,
        phone_number STRING,
        ssn STRING
        """
    )

    employee_details = [
        (1, 'Senthil', 'Kumar', 1000.0, 'India', '+91 807 278 6328,+91 986 278 8576', '456 87 9985'),
        (2, 'John', 'Cena', 800.0, 'United States', '+44 807 278 8956,+44 935 457 3478', '856 87 6955'),
        (3, 'Brock', 'Lesnar', 1200.0, 'United States', '+1 968 278 7368,+1 748 359 2214', '896 87 3678'),
        (4, 'Chris', 'Bumsted', 8900.0, 'Canada', '+61 458 295 3586', '689 35 4586'),
    ]

    employee_df_phones = spark.createDataFrame(
        data=employee_details,
        schema="""
        id INT,
        first_name STRING,
        last_name STRING,
        salary float,
        nationality STRING,
        phone_numbers STRING,
        ssn STRING
        """
    )

except Exception as error:
    raise error
