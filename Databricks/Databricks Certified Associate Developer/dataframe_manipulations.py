import create_dataframes
import utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


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

    column_list = ["firstname", "lastname"]

    customer_df.select(
        [col for col in customer_df.columns]
    ).show(5)

    # Selecting all columns
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


def add_columns():
    print(customer_df.columns)
    print("Length:", len(customer_df.columns))
    df = customer_df.withColumn("website", lit("http://connectcommunity.pythonanywhere.com/"))
    print(customer_df.columns)
    print("Length after adding a column:", len(df.columns))
    df.show(5)

    df = customer_df.select(
        "firstname",
        "lastname"
    ).withColumn(
        "Name", concat("firstname", "lastname")
    )

    df.show(5)

    # Only lastname will be selected
    df = customer_df.select(
        "firstname",
        "lastname"
    ).withColumn(
        "Name", concat_ws("firstname", "lastname")
    )

    df.show(5)

    df = customer_df.select(
        "firstname",
        "lastname"
    ).withColumn(
        "Name", concat_ws(" ", "firstname", "lastname")
    )

    df.show(5)

    print("Adding column by computing from existing column")

    customer_id_df = customer_df.select(
        customer_df.customer_id,
        customer_df.firstname,
        customer_df.lastname
    ).withColumn(
        "New ID", expr("customer_id + 1000000"),
    ).withColumn(
        "New lastname", expr("lastname||' '||firstname")
    )

    customer_id_df.show(10)


def remove_columns():
    columns_to_be_removed = []
    for c in customer_df.columns:
        if not c.__contains__('name'):
            # if 'name' not in c:
            columns_to_be_removed.append(c)
    print("Columns to be removed:", columns_to_be_removed)
    df = customer_df.drop(*columns_to_be_removed)
    print("DF columns:", df.columns)

    print("\nRemoving columns from df by passing string args")
    print(customer_df.columns)
    df = customer_df.drop("demographics", "firstname", "lastname")
    print(df.columns)

    # Works only when passing single column inside drop function

    print("\nRemoving columns from df by passing df value")
    print(customer_df.columns)
    df = customer_df.drop(customer_df.firstname)
    print(df.columns)

    print("\nRemoving columns from df by passing col args")
    print(customer_df.columns)
    df = customer_df.drop(col('firstname'))
    print(df.columns)


def arithmetic_operations():
    web_sales_df.show(10)
    sales_stat_df = web_sales_df.select(
        "ws_order_number",
        "ws_item_sk",
        "ws_quantity",
        "ws_net_paid",
        "ws_net_profit",
        "ws_wholesale_cost"
    )

    sales_stat_df.show(10)

    sales_performance_df = sales_stat_df.withColumn(
        "expected_net_paid", col("ws_quantity") * col("ws_wholesale_cost")
    ).withColumn(
        "test_col", expr("ws_quantity + 10000000")
    )

    sales_performance_df.show(10)

    sales_performance_df = sales_stat_df.withColumn(
        "expected_net_paid", col("ws_quantity") * col("ws_wholesale_cost")
    ).withColumn(
        "calculated_profit", col("ws_net_paid") - col("expected_net_paid")
    ).withColumn(
        "unit_price", expr("ws_wholesale_cost/ws_quantity")
    ).withColumn(
        "rounded_unit_price", round(col("unit_price"), 2)
    ).withColumn(
        "brounded_unit_price", bround(col("unit_price"), 3)
    ).select(
        "expected_net_paid", "calculated_profit", "unit_price", "rounded_unit_price", "brounded_unit_price"
    )

    sales_performance_df.show(10)


def filter_df():
    customer_df.show(10)
    df = customer_df.filter(
        year(col("birthdate")) > 1980
    ).where(
        month("birthdate") == 11
    ).filter(
        (col("gender") == 'M') & (lower(col("salutation")) != 'miss')
    ).where(
        "is_preffered_customer == 'Y' and day(birthdate)==21"
    )
    df.show(10)
    print(customer_df.count())
    print(df.count())


def df_to_list(df: DataFrame):
    print("Converting dataframe to python list object")
    # print("Refer:https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-column-to-python-list/")
    # import webbrowser
    # url = 'https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-column-to-python-list/'
    # webbrowser.open_new(url=url)
    # converts to list of Row objects
    df = customer_df.take(10)
    print(type(df))
    print(df)
    for i in df:
        print(i.customer_id, " - ", i.demographics.credit_rating)

    print("\ndf to list using rdd map using index")
    df = customer_df.limit(10)
    df_list = df.rdd.map(lambda x: x[3]).collect()
    print(df_list)

    print("\ndf to list using rdd map using column name")
    df_list = df.rdd.map(lambda x: x.customer_id).collect()
    print(df_list)

    print("\ndf to list using rdd flatmap using column name")
    df_list = df.select("customer_id").rdd.flatMap(lambda x: x).collect()
    print(df_list)

    print("\ndf to list using rdd flatmap selecting all columns")
    df_list = df.select("*").rdd.flatMap(lambda x: x).collect()
    print(df_list)

    print("\nUsing map collect - Gives the list of Row objects")
    df_list = df.select("*").collect()
    print(df_list)
    df.coalesce(1).write.format("json").save(utils.data_directory+"json_test_data")

    # df_list = df.select("customer_id").toPandas()
    # print(df_list)



try:
    with utils.spark as spark:
        # dfs = create_dataframes.get_dataframes(spark=spark)
        # display_dataframes(dataframes=dfs)
        # customer_df = create_dataframes.get_customer_df(spark=spark)
        web_sales_df: DataFrame = create_dataframes.get_web_sales_df(spark=spark)
        # address_df: DataFrame = create_dataframes.get_address_df(spark=spark)
        # item_df: DataFrame = create_dataframes.get_item_df(spark=spark)
        customer_df: DataFrame = create_dataframes.get_customer_df(spark=spark)

        # selecting_columns()
        # renaming_columns()
        # changing_column_types()
        # add_columns()
        # remove_columns()
        # arithmetic_operations()
        # filter_df()
        df_to_list(customer_df)


except Exception as error:
    raise error
