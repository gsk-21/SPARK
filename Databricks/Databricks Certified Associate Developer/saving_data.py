import datetime
import create_dataframes
import utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


def save_df():
    customer_with_address = customer_df.na.drop("any").join(
        address_df, customer_df.address_id == address_df.address_id,
        how="inner"
    ).withColumn(
        "Name", concat_ws(' ', customer_df.firstname, customer_df.lastname)
    ).select(
        "Name",
        "customer_id",
        "demographics",
        address_df["*"]
    )

    sale_with_item = web_sales_df.drop("any").join(
        item_df, web_sales_df['ws_item_sk'] == item_df['i_item_sk']
    ).selectExpr("ws_bill_customer_sk customer_id",
                 "ws_ship_addr_sk ship_address_id",
                 "i_product_name item_name",
                 "i_category item_category",
                 "ws_quantity quantity",
                 "ws_net_paid net_paid")

    customer_purchases = customer_with_address.join(
        sale_with_item, customer_with_address['customer_id'] == sale_with_item['customer_id']
    ).select(
        customer_with_address['*'],
        sale_with_item['*']
    ).drop(sale_with_item.customer_id)

    customer_purchases.show(10)
    print("customer purchases count:", customer_purchases.count())

    # customer_purchases.groupBy("Name").count().orderBy(col("count").desc()).show(500)
    print("Total partitions:", customer_purchases.rdd.getNumPartitions())

    path_to_df = utils.data_directory + "/customer_purchases"

    print("\nSaving the dataframe without specifying the format.\nDefault format is parquet")
    customer_purchases.write.option("path", path_to_df).save()
    print("\nSaving the dataframe without specifying the format using append mode.\nDefault format is parquet")
    customer_purchases.write.mode("append").option("path", path_to_df).save()

    print("Saving json data using overwrite mode by repartitioning to 1")
    json_path = utils.data_directory + "/customer_purchases_json"
    customer_purchases.repartition(1).write.format(
        "json"
    ).mode(
        saveMode="overwrite"
    ).option(
        "path", json_path
    ).save()

    df = spark.read.json(json_path)
    print("Count check : ", df.count())


def save_by_partition():
    customer_with_address = customer_df.na.drop("any").join(
        address_df, customer_df.address_id == address_df.address_id,
        how="inner"
    ).withColumn(
        "Name", concat_ws(' ', customer_df.firstname, customer_df.lastname)
    ).select(
        "Name",
        "customer_id",
        "demographics",
        address_df["*"]
    )

    sale_with_item = web_sales_df.drop("any").join(
        item_df, web_sales_df['ws_item_sk'] == item_df['i_item_sk']
    ).selectExpr("ws_bill_customer_sk customer_id",
                 "ws_ship_addr_sk ship_address_id",
                 "i_product_name item_name",
                 "i_category item_category",
                 "ws_quantity quantity",
                 "ws_net_paid net_paid")

    customer_purchases = customer_with_address.join(
        sale_with_item, customer_with_address['customer_id'] == sale_with_item['customer_id']
    ).select(
        customer_with_address['*'],
        sale_with_item['*']
    ).drop(sale_with_item.customer_id)

    customer_purchases.show(5)

    # customer_purchases.distinct().show()

    partitions = customer_purchases.select("city").distinct().dropna("any").orderBy("city").collect()

    customer_purchases_partitioned = utils.data_directory + "customer_purchases_partitioned"

    customer_purchases.repartition(1).write.format(
        "json"
    ).mode(
        "overwrite"
    ).partitionBy(
        "city"
    ).save(customer_purchases_partitioned)

    for partition in partitions:
        city = partition['city']
        df = spark.read.json(customer_purchases_partitioned + "/city=" + city)
        print(f"{city} - ".rjust(25," "), df.count())

try:
    with utils.spark as spark:
        # dfs = create_dataframes.get_dataframes(spark=spark)
        # display_dataframes(dataframes=dfs)
        # customer_df = create_dataframes.get_customer_df(spark=spark)
        web_sales_df: DataFrame = create_dataframes.get_web_sales_df(spark=spark)
        address_df: DataFrame = create_dataframes.get_address_df(spark=spark)
        item_df: DataFrame = create_dataframes.get_item_df(spark=spark)
        customer_df: DataFrame = create_dataframes.get_customer_df(spark=spark)

        # save_df()
        save_by_partition()

except Exception as error:
    raise error
