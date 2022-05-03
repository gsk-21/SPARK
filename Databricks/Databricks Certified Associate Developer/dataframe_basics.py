import json
from pyspark.sql import SparkSession
# from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, DateType, LongType
from pyspark.sql.functions import struct, when, col, lit, expr, concat

import utils


def struct_field_df():
    """
    StructType - To define a schema
    StructType - Similar to object creation
    StructField - Fields present inside the StructType Object
    StructField Syntax - StructField(name,dataType,nullable:bool)
    """
    struct_type = StructType(
        [
            StructField("ID", IntegerType(), False),
            StructField("Name", StringType(), True)
        ]
    )
    print(struct_type)

    struct_type.add(StructField("dob", DateType(), False))
    print(struct_type)

    struct_fields = [StructField("ID", IntegerType(), False), StructField("Name", StringType(), True)]
    struct_type = StructType(
        struct_fields
    )
    print(struct_type)

    print("\nReading JSON data using Struct Type")
    customer_df_struct_type = StructType(
        [
            StructField("address_id", IntegerType(), True),
            StructField("birth_country", StringType()),
            StructField("birthdate", DateType()),
            StructField("customer_id", IntegerType(), False),
            StructField("demographics", StructType([
                StructField("buy_potential", StringType()),
                StructField("credit_rating", StringType()),
                StructField("education_status", StringType()),
                StructField("income_range", ArrayType(IntegerType())),
                StructField("purchase_estimate", LongType()),
                StructField("vehicle_count", LongType())
            ])),
            StructField("email_address", StringType()),
            StructField("firstname", StringType()),
            StructField("gender", StringType()),
            StructField("is_preferred_customer", StringType()),
            StructField("lastname", StringType()),
            StructField("salutation", StringType())
        ]
    )

    customer_df = spark.read.format("json").schema(customer_df_struct_type).load(utils.json_file)
    customer_df.show()
    customer_df.printSchema()

    print("\nReading JSON data using Struct Type")

    '''
    If a column name specified in the struct field is not available in the json file,
    Then it fills the value with null
    'address_' will have null values
    '''
    demographic_list = [
        StructField("buy_potential", StringType()),
        StructField("credit_rating", StringType()),
        StructField("education_status", StringType()),
        StructField("income_range", ArrayType(IntegerType())),
        StructField("purchase_estimate", LongType()),
        StructField("vehicle_count", LongType())
    ]
    struct_field_list = [
        StructField("address_id", IntegerType(), True),
        StructField("birth_country", StringType()),
        StructField("birthdate", DateType()),
        StructField("customer_id", IntegerType(), False),
        StructField("demographics", StructType(demographic_list)),
        StructField("email_address", StringType()),
        StructField("firstname", StringType()),
        StructField("gender", StringType()),
        StructField("is_preferred_customer", StringType()),
        StructField("lastname", StringType()),
        StructField("salutation", StringType())
    ]
    customer_df_struct_type = StructType(
        struct_field_list
    )

    customer_df = spark.read.format("json").schema(customer_df_struct_type).load(utils.json_file)
    # customer_df = spark.read.format("json").load(json_file)
    customer_df.show()
    customer_df.printSchema()
    print(customer_df.schema.fieldNames())
    print(customer_df.schema.fields)

    for i in customer_df.schema.fieldNames():
        print(i, "-", type(i))
    print("\nfirstname - ", "firstname" in customer_df.schema.fieldNames())

    print("\nSchema Fields")
    for i in customer_df.schema.fields:
        print(i.name, " : ", i.dataType)

    first_name_sf = StructField("firstname", StringType())
    print("\nfirstname sf -", first_name_sf in customer_df.schema.fields)
    first_name_sf = StructField("firstname", StringType(), True)
    print("\nfirstname True sf -", first_name_sf in customer_df.schema.fields)
    first_name_sf = StructField("firstname", StringType(), False)
    print("\nfirstname False sf -", first_name_sf in customer_df.schema.fields)


def ddl_df():
    print("Using DDL schema")
    customer_df_schema_ddl = """
                             address_id INT,
                             birth_country STRING,
                             birthdate DATE,
                             customer_id INT,
                             demographics STRUCT<buy_potential:STRING,
                                                 credit_rating:STRING,
                                                 education_status:STRING,
                                                 income_range:ARRAY<INT>,
                                                 purchase_estimate:INT,
                                                 vehicle_count:INT>,

                             email_address STRING,
                             firstname STRING,
                             gender STRING,
                             is_preferred_customer STRING,
                             lastname STRING,
                             salutation STRING
                            """

    print("Reading JSON File")
    customer_df = spark.read.format("json").schema(customer_df_schema_ddl).load(utils.json_file)
    customer_df.show()


def json_df():
    ddl_path = utils.data_directory + "emp_ddl.json"
    print(json.load(open(ddl_path)))
    json_schema = StructType.fromJson(json.load(open(ddl_path)))

    print("\nCreating dataframe")
    df = spark.createDataFrame(data=data, schema=json_schema)
    df.show()

    # distribute the data across multiple nodes
    # Allows creating df from an empty list and non-empty list
    print("\nCreating dataframe")
    df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema=json_schema)
    df.show()


def user_defined_df():
    udf_schema = StructType(
        [
            StructField("First Name", StringType()),
            StructField("Middle Name", StringType()),
            StructField("Last Name", StringType()),
            StructField("ID", StringType()),
            StructField("Gender", StringType()),
            StructField("Salary", IntegerType())
        ]
    )
    udf = spark.createDataFrame(data=data, schema=udf_schema, verifySchema=True)
    udf.show()
    udf.printSchema()

    structure_schema = StructType(
        [
            StructField("Name", StructType(
                [
                    StructField("first name", StringType()),
                    StructField("middle name", StringType()),
                    StructField("last name", StringType())
                ]
            )),
            StructField("ID", StringType()),
            StructField("Gender", StringType()),
            StructField("Salary", IntegerType())

        ]
    )

    udf = spark.createDataFrame(data=structure_data, schema=structure_schema)
    udf.printSchema()
    dataframe_display(udf)


def update_df():
    udf_schema = StructType(
        [
            StructField("first_name", StringType()),
            StructField("middle_name", StringType()),
            StructField("last_name", StringType()),
            StructField("ID", StringType()),
            StructField("Gender", StringType()),
            StructField("Salary", IntegerType())
        ]
    )
    udf = spark.createDataFrame(data=data, schema=udf_schema, verifySchema=True)
    udf.show()
    udf.printSchema()

    print("Using expr in concatenating columns")

    updated_df = udf.withColumn("salary grade",
                                when(col("salary").cast(IntegerType()) < 2000, "Low")
                                .when(
                                    (col("salary").cast(IntegerType()) <= 3000) &
                                    (col("salary").cast(IntegerType()) >= 2000),
                                    "Medium"
                                )
                                .otherwise("High")
                                ).withColumn("Name", expr("first_name||' '||middle_name||' '||last_name")). \
        drop("first_name", "middle_name", "last_name").select("Name", "salary", "salary grade")
    updated_df.show()

    print("Using concat() function in concatenating columns")
    updated_df = udf.withColumn("salary grade",
                                when(col("salary").cast(IntegerType()) < 2000, "Low")
                                .when(
                                    (col("salary").cast(IntegerType()) <= 3000) &
                                    (col("salary").cast(IntegerType()) >= 2000), "Medium"
                                )
                                .otherwise("High")
                                ).withColumn(
                                "Name",
                                concat(udf.first_name, lit(' '), udf.middle_name, lit(' '), udf.last_name)
                                )
    updated_df.show()


def dataframe_display(df):
    print("\nDisplaying first 20 rows")
    df.show()

    # Show full contents of DataFrame (PySpark)
    print("\nDisplaying all the contents of the column without truncating")
    df.show(truncate=False)

    # Show top 2 rows and full column contents (PySpark)
    print("Displaying only top 2 rows without truncating columns")
    df.show(2, truncate=False)

    # Shows top 2 rows and only 25 characters of each column (PySpark)
    print("Displaying only top 2 rows with column truncated to 5 characters")
    df.show(2, truncate=5)

    # Show rows vertically (one line per column value) (PySpark)
    print("Displaying only top 3 rows vertically with column truncated to 25 characters")
    df.show(n=3, truncate=25, vertical=True)


try:
    with utils.spark as spark:
        data = utils.data
        structure_data = utils.structure_data

        dataframe = spark.read.format("json").load(utils.json_file)
        dataframe.printSchema()

        # print("Struct Field DF:\n")
        # struct_field_df()

        # print("DDL DF:\n")
        # ddl_df()

        # print("DF from JSON DDL:")
        # json_df()

        # print("User Defined DF:\n")
        # user_defined_df()
        #
        print("Update DF:")
        update_df()

except Exception as error:
    raise error
