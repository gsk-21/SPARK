import create_dataframes
import utils
from pyspark.sql.functions import col, column, expr, concat
from pyspark.sql.types import *


def display_dataframes(dataframes):
    customer_df = dataframes['customer_df']
    web_sales_df = dataframes['web_sales_df']
    address_df = dataframes['address_df']
    item_df = dataframes['item_df']

    print("\ncustomer df:", customer_df)

    print("\ncustomer df count:", customer_df.count())

    customer_df.show(5)

    print("\nweb_sales df:", web_sales_df)

    print("\nweb_sales df count:", web_sales_df.count())

    web_sales_df.show(5)

    print("\naddress df:", address_df)

    print("\naddress df count:", address_df.count())

    address_df.show(5)

    print("\nitem df:", item_df)

    print("\nitem df count:", item_df.count())

    item_df.show(5)


def selecting_columns():
    customer_df.select("firstname", "lastname", "demographics", "demographics.credit_rating"
                       ).show(n=5, truncate=False)

    customer_df.select(
        "firstname",
        'firstname',
        col("firstname"),
        column("firstname"),
        expr("firstname"),
        expr("concat(firstname,' ',lastname) name"),
        customer_df["firstname"]
    ).show(5)

    customer_df.select(customer_df.colRegex("`^.*name*`")).show(5)

    customer_df.select(
        customer_df.firstname
    ).show(5)

    # Selecting all columns
    column_list = ["firstname", "lastname"]

    customer_df.select(
        [col for col in customer_df.columns]
    ).show(5)

    customer_df.select("*").show(5)

    customer_df.select(column_list).show(5)

    customer_df.select(customer_df.columns).show(5)

    print(customer_df.columns)

    # using selectExpr
    print("\nUsing selectExpr")
    customer_df.selectExpr("birthdate birthday", "year(birthdate) year").show(5)


def renaming_columns():
    print("\nRenaming columns")
    customer_df.withColumnRenamed("firstname", "Name").select("Name").show(5)

    # Belowe one does not work
    # customer_df.withColumnRenamed(col("firstname"), "Name").select("Name").show(5)
    # do nothing
    customer_df.withColumnRenamed("first_name", "Name")


def changing_column_types():
    # Throws error
    # "address_id".cast("long")

    df = customer_df.select(
        col("address_id").cast("long"),
        col("birthdate").cast(StringType())
    )

    df.show(5)

    df = customer_df.selectExpr(
        "cast(address_id as long)",
        "cast(birthdate as string)"
    )

    df.show(5)


try:
    with utils.spark as spark:
        # dfs = create_dataframes.get_dataframes(spark=spark)
        # display_dataframes(dataframes=dfs)
        # customer_df = create_dataframes.get_customer_df(spark=spark)
        # web_sales_df = create_dataframes.get_web_sales_df(spark=spark)
        # address_df = create_dataframes.get_address_df(spark=spark)
        # item_df = create_dataframes.get_item_df(spark=spark)
        customer_df = create_dataframes.get_customer_df(spark=spark)
        # selecting_columns()
        # renaming_columns()
        changing_column_types()

except Exception as error:
    raise error
