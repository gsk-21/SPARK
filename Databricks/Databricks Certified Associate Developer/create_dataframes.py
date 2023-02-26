import utils
from pyspark.sql.types import *


def get_customer_df(spark):
    customer_df_schema_ddl = """
                                address_id INT,birth_country STRING,birthdate date,customer_id INT,
                                demographics STRUCT<buy_potential: STRING, credit_rating: STRING,
                                education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT,
                                vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,
                                is_preffered_customer STRING,lastname STRING,salutation STRING
                                """
    customer_df = spark.read.format("json").schema(customer_df_schema_ddl).load(utils.json_file)
    customer_df.printSchema()
    print("customer df read success!\n")
    return customer_df


def get_web_sales_df(spark):
    web_sales_schema = """
                            ws_sold_date_sk LONG,ws_sold_time_sk LONG,ws_ship_date_sk LONG,
                            ws_item_sk LONG,ws_bill_customer_sk LONG,ws_bill_cdemo_sk LONG,
                            ws_bill_hdemo_sk LONG,ws_bill_addr_sk LONG,ws_ship_customer_sk LONG,ws_ship_cdemo_sk LONG,
                            ws_ship_hdemo_sk LONG,ws_ship_addr_sk LONG,ws_web_page_sk LONG,ws_web_site_sk LONG,
                            ws_ship_mode_sk LONG,ws_warehouse_sk LONG,ws_promo_sk LONG,ws_order_number LONG,
                            ws_quantity INT,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),
                            ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),
                            ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),
                            ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),
                            ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),
                            ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),
                            ws_net_profit decimal(7,2)
                        """
    web_sales_df = spark.read.options(header="true", delimiter=",", inferSchema="false").schema(
        web_sales_schema).csv(utils.csv_file)
    web_sales_df.printSchema()
    print("web sales df read success!\n")
    return web_sales_df


def get_address_df(spark):
    address_df = spark.read.parquet(utils.parquet_file)
    address_df.printSchema()
    print("address df read success!\n")
    return address_df


def get_item_df(spark):
    item_schema = """
                        i_item_sk LONG,i_item_id STRING,i_rec_start_date date,i_rec_end_date date,
                        i_item_desc STRING,i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),
                        i_brand_id INT,i_brand STRING,i_class_id INT,i_class STRING,i_category_id INT,
                        i_category STRING,i_manufact_id INT,i_manufact STRING,i_size STRING,i_formulation STRING,
                        i_color STRING,i_units STRING,i_container STRING,i_manager_id INT,i_product_name STRING
                     """
    item_df = spark.read.options(header="true", delimiter="|", inferSchema="false").schema(item_schema).csv(utils.dat_file)
    item_df.printSchema()
    print("item df read success!\n")
    return item_df


def get_dataframes(spark):
    try:
        customer_df_schema_ddl = """
                                    address_id INT,birth_country STRING,birthdate date,customer_id INT,
                                    demographics STRUCT<buy_potential: STRING, credit_rating: STRING,
                                    education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT,
                                    vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,
                                    is_preffered_customer STRING,lastname STRING,salutation STRING
                                    """

        web_sales_schema = """
                                ws_sold_date_sk LONG,ws_sold_time_sk LONG,ws_ship_date_sk LONG,
                                ws_item_sk LONG,ws_bill_customer_sk LONG,ws_bill_cdemo_sk LONG,
                                ws_bill_hdemo_sk LONG,ws_bill_addr_sk LONG,ws_ship_customer_sk LONG,ws_ship_cdemo_sk LONG,
                                ws_ship_hdemo_sk LONG,ws_ship_addr_sk LONG,ws_web_page_sk LONG,ws_web_site_sk LONG,
                                ws_ship_mode_sk LONG,ws_warehouse_sk LONG,ws_promo_sk LONG,ws_order_number LONG,
                                ws_quantity INT,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),
                                ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),
                                ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),
                                ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),
                                ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),
                                ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),
                                ws_net_profit decimal(7,2)
                            """

        item_schema = """
                            i_item_sk LONG,i_item_id STRING,i_rec_start_date date,i_rec_end_date date,
                            i_item_desc STRING,i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),
                            i_brand_id INT,i_brand STRING,i_class_id INT,i_class STRING,i_category_id INT,
                            i_category STRING,i_manufact_id INT,i_manufact STRING,i_size STRING,i_formulation STRING,
                            i_color STRING,i_units STRING,i_container STRING,i_manager_id INT,i_product_name STRING
                         """

        customer_df = spark.read.format("json").schema(customer_df_schema_ddl).load(utils.json_file)
        customer_df.printSchema()
        print("customer df read success!\n")

        web_sales_df = spark.read.options(header="true", delimiter="|", inferSchema="false").schema(
            web_sales_schema).csv(utils.csv_file)
        web_sales_df.printSchema()
        print("web sales df read success!\n")

        address_df = spark.read.parquet(utils.parquet_file)
        address_df.printSchema()

        print("address df read success!\n")

        item_df = spark.read.options(header="true", delimiter="|", inferSchema="false").schema(item_schema).csv(
            utils.dat_file)
        item_df.printSchema()

        print("item df read success!\n")

        dataframes = {
            'customer_df': customer_df,
            'web_sales_df': web_sales_df,
            'address_df': address_df,
            'item_df': item_df
        }

        return dataframes

    except Exception as error:
        raise error
