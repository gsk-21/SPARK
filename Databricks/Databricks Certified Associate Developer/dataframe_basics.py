import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, DateType, LongType
from pyspark.sql.functions import struct, when, col


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

    customer_df = spark.read.format("json").schema(customer_df_struct_type).load(json_file)
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

    customer_df = spark.read.format("json").schema(customer_df_struct_type).load(json_file)
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
    customer_df = spark.read.format("json").schema(customer_df_schema_ddl).load(json_file)
    customer_df.show()


def json_df():
    ddl_path = data_path + "emp_ddl.json"
    print(json.load(open(ddl_path)))
    json_schema = StructType.fromJson(json.load(open(ddl_path)))

    print("\nCreating dataframe")
    df = spark.createDataFrame(data=data, schema=json_schema)
    df.show()

    # distribute the data across multiple nodes
    # Allows to create df from an empty list and non-empty list
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
    print("Displaying only top 2 rows with column truncated to 25 characters")
    df.show(2, truncate=5)

    # Shows rows vertically (one line per column value) (PySpark)
    print("Displaying only top 3 rows vertically with column truncated to 25 characters")
    df.show(n=3, truncate=25, vertical=True)


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
try:

    data_path = "C:\\Users\\$en\\Documents\\GitHub\\SPARK\\Databricks\\Data\\"
    # spark = SparkSession.builder.master("local").appName("Databricks").getOrCreate()
    with SparkSession.builder.master("local").appName("Databricks").getOrCreate() as spark:
        json_file = data_path + "customer.json"
        parquet_file = data_path + "address.parquet"
        csv_file = data_path + "web_sales.csv"
        dat_file = data_path + "item.dat"
        tab_file = data_path + "web_sales_excel_ft.prn"
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

        # df = spark.read.format("json").load(json_file)
        # df.printSchema()

        # print("Struct Field DF:\n")
        # struct_field_df()

        # print("DDL DF:\n")
        # ddl_df()

        # print("User Defined DF:\n")
        # user_defined_df()

        print("DF from JSON DDL:")
        json_df()










































except Exception as error:
    raise error
