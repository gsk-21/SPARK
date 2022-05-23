import create_dataframes
import utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


def get_count(df: DataFrame):
    print("\ncount:", df.count())


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
    df.coalesce(1).write.format("json").save(utils.data_directory + "json_test_data")

    # df_list = df.select("customer_id").toPandas()
    # print(df_list)


def drop_df_rows():
    data = [
        [1, 'Monday'],
        [1, 'Monday'],
        [2, 'Tuesday'],
        [3, 'Tuesday'],
        [3, 'Wednesday'],
        [4, 'Thursday'],
        [5, 'Friday'],
        [6, 'Saturday'],
        [7, 'Sunday']
    ]

    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("Days", StringType())
        ]
    )

    df = spark.createDataFrame(schema=schema, data=data)
    df.show()

    print("\nDistinct Values")

    df.distinct().orderBy('id').show()

    print("\nDropping duplicates based on days ")

    df.dropDuplicates(["days"]).orderBy("id").show()

    print("\nDropping duplicates based on id ")

    df.drop_duplicates(['id']).orderBy("id").show()

    print("\nDropping duplicates based on ['id', 'days'] ")

    df.dropDuplicates(['id', 'days']).orderBy("id").show()

    print("\n Dropping duplicates without passing any args (Similar to distinct)")

    df.drop_duplicates().orderBy("id").show()

    # Throws error
    # print("\nDropping duplicates based on ['id', 'days', 'month'] ")
    #
    # df.dropDuplicates(['id', 'days', 'month']).show()


def filtering_null():
    df = customer_df.selectExpr("salutation", "firstname", "lastname", "email_address", "year(birthdate) birthyear ")
    df.show(10)
    print("\ndf count:", df.count())

    print("\nWith Single column:")

    df_null = df.where(
        "salutation is null"
    )
    print("\ndf count with null (is null):", df_null.count())

    df_null = df.filter(
        df.salutation.isNull()
    )
    print("\ndf count with null [df.salutation.isNull()] :", df_null.count())

    df_null = df.where(
        col("salutation").isNull()
    )
    print("\ndf count with null [col('salutation').isNull()] :", df_null.count())

    df_not_null = df.where(
        "salutation is not null"
    )
    print("\ndf count with no null (is not null) :", df_not_null.count())

    df_not_null = df.where(
        df.salutation.isNotNull()
    )
    print("\ndf count with no null [df.salutation.isNotNull] :", df_not_null.count())

    df_not_null = df.where(
        col("salutation").isNotNull()
    )
    print("\ndf count with no null [col(salutation).isNotNull] :", df_not_null.count())

    print("\nWith Multiple columns:")

    df_null = df.where(
        "salutation is null and birthyear is not null"
    )
    print("\ndf count with null (is null):", df_null.count())

    df_null = df.filter(
        df.salutation.isNull() & df.firstname.isNull()
    )
    print("\ndf count with null [df.salutation.isNull()] :", df_null.count())

    df_null = df.where(
        col("salutation").isNull() & col("email_address").isNull()
    )
    print("\ndf count with null [col('salutation').isNull()] :", df_null.count())

    df_not_null = df.where(
        "salutation is not null and lastname is not null"
    )
    print("\ndf count with no null (is not null) :", df_not_null.count())

    df_not_null = df.where(
        df.salutation.isNotNull() & df.firstname.isNotNull()
    )
    print("\ndf count with no null [df.salutation.isNotNull] :", df_not_null.count())

    df_not_null = df.where(
        col("salutation").isNotNull() & col("birthyear").isNotNull() & df['firstname'].isNull()
    )
    print("\ndf count with no null [col(salutation).isNotNull] :", df_not_null.count())


def handling_null():
    df = customer_df.selectExpr(
        "salutation", "firstname", "lastname", "email_address", "year(birthdate) birthyear "
    ).orderBy("salutation").limit(200)
    df.show(200)

    get_count(df)
    # Drops the row which has all the column values has null
    null_removed = df.na.drop(how="all")
    null_removed.show(200)
    get_count(null_removed)

    # Drops the row if any of the column value is null
    null_removed = df.na.drop(how="any")
    null_removed.show(200)
    get_count(null_removed)

    # Drops the row if all the columns specified in the list has null
    null_removed = df.na.drop(how="all", subset=["firstname", "lastname"])
    null_removed.show(200)
    get_count(null_removed)

    # Drops the row if any of the columns specified in the list has null
    null_removed = df.na.drop(how="any", subset=["firstname", "lastname"])
    null_removed.show(200)
    get_count(null_removed)

    # Fills all the null values with whatever given
    null_filled = df.na.fill("sen's-spark-learning")
    null_filled.show(200)

    # fills null values based on the values given in the dictionary
    null_filled = df.na.fill({
        "salutation": "UNKNOWN",
        "firstname": "Senthil"
    })
    null_filled.show(200)

    # Learn replace***************


def sort_rows():
    df = customer_df.na.drop("any")
    cols = ["firstname", "lastname", "birthdate"]

    df.sort("firstname").select(cols).show(50)

    df.sort(expr("firstname")).select(cols).show(50)

    df.sort("firstname", col("lastname").desc()).select(cols).show(50)

    df.sort(expr("firstname")).orderBy(
        expr('month("birthdate")')
    ).orderBy(
        col("lastname").desc()
    ).select("firstname", "lastname", "birthdate").show(50)


def group_by():
    customer_purchases = web_sales_df.selectExpr("ws_bill_customer_sk customer_id", "ws_item_sk item_id").limit(100)
    df = customer_purchases.groupBy("customer_id")
    print(df)
    df.agg(count("item_id")).show()
    df.agg(sum("item_id").alias("item_sum")).show()
    df.agg(
        sum("item_id").alias("item_sum"),
        count("item_id").alias("item_count")
    ).orderBy(
        "item_count"
    ).show()


def df_statistics():
    web_sales_df.select(max("ws_sales_price")).show()
    web_sales_df.select(min(col("ws_sales_price"))).show()
    print("\nUsing select:")
    web_sales_df.select(
        min(col("ws_sales_price")).alias("min"),
        max(col("ws_sales_price")).alias("max"),
        sum(col("ws_sales_price")).alias("sum"),
        count(col("ws_sales_price")).alias("count"),
        mean(col("ws_sales_price")).alias("mean"),
        avg(col("ws_sales_price")).alias("avg")
    ).show()

    print("\nUsing agg:")

    web_sales_df.agg(
        min(col("ws_sales_price")).alias("min"),
        max(col("ws_sales_price")).alias("max"),
        sum(col("ws_sales_price")).alias("sum"),
        count(col("ws_sales_price")).alias("count"),
        mean(col("ws_sales_price")).alias("mean"),
        avg(col("ws_sales_price")).alias("avg")
    ).show()

    print("\ncustomer_df.describe()")
    customer_df.describe().show()

    print("\ncustomer_df.summary()")
    customer_df.summary().show()

    print("customer_df count (Includes null values):", customer_df.count())
    df = customer_df.select(count("firstname").alias("count"))
    firstname_count = df.collect().pop(0)['count']
    print("\ncount of firstname alone (Excludes null values):", firstname_count)


def joins():
    inner_join_df = customer_df.join(address_df, customer_df.address_id == address_df.address_id, "inner")
    print("inner join :", inner_join_df.count())

    inner_join_df = customer_df.join(address_df, customer_df['address_id'] == address_df['address_id'])
    print("inner join :", inner_join_df.count())

    df = web_sales_df.join(customer_df, col("customer_id") == col("ws_bill_customer_sk"))
    print("Without any join type:", df.count())
    # df.show(100)

    df = web_sales_df.join(customer_df, web_sales_df['ws_bill_customer_sk'] == customer_df['customer_id'], "right")
    print("Right join:", df.count())
    print("Column with null values in left table:", df.filter("ws_bill_customer_sk is null").count())
    print("Column with null values in right table:", df.filter("customer_id is null").count())

    df = web_sales_df.join(customer_df, web_sales_df['ws_bill_customer_sk'] == customer_df['customer_id'], "left")
    print("left join:", df.count())
    print("Column with null values in left table:", df.filter("ws_bill_customer_sk is null").count())
    print("Column with null values in right table:", df.filter("customer_id is null").count())

    df.sort(col("customer_id").asc_nulls_first()).show(10)


def append_rows():
    df1 = customer_df.select("firstname", "lastname", "customer_id").withColumn(
        "from", lit("df1")
    )

    df2 = customer_df.select("lastname","firstname", "customer_id").withColumn(
        "from", lit("df2")
    )

    union_df = df1.union(df2)

    print("union (Combines rows if the column data type matches)")
    union_df.sort(col("customer_id")).show(10)

    df1 = customer_df.select("firstname", "lastname", "customer_id").withColumn(
        "from", lit("df1")
    )

    df2 = customer_df.select("lastname","firstname", "customer_id").withColumn(
        "from", lit("df2")
    )

    union_df = df1.unionByName(df2)

    print("unionByName (Combines rows based on the column names)")
    union_df.sort(col("customer_id")).show(10)

    print("unionAll is deprecated")
    df1.unionAll(df2).sort(col("customer_id")).show(10)

    print("Union results in duplicate")

try:
    with utils.spark as spark:
        # dfs = create_dataframes.get_dataframes(spark=spark)
        # display_dataframes(dataframes=dfs)
        # customer_df = create_dataframes.get_customer_df(spark=spark)
        web_sales_df: DataFrame = create_dataframes.get_web_sales_df(spark=spark)
        address_df: DataFrame = create_dataframes.get_address_df(spark=spark)
        # item_df: DataFrame = create_dataframes.get_item_df(spark=spark)
        customer_df: DataFrame = create_dataframes.get_customer_df(spark=spark)

        # selecting_columns()
        # renaming_columns()
        # changing_column_types()
        # add_columns()
        # remove_columns()
        # arithmetic_operations()
        # filter_df()
        # df_to_list(customer_df)
        # drop_df_rows()
        # filtering_null()
        # handling_null()
        # sort_rows()
        # group_by()
        # df_statistics()
        # joins()
        append_rows()


except Exception as error:
    raise error
