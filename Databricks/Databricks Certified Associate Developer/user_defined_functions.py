import datetime
import create_dataframes
import utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


def string_concat(sep: str, str1: str, str2: str):
    return str(str1 + sep + str2)


def user_functions():
    string_concat_udf = udf(string_concat, StringType())
    print("using udf as dataframe function")
    df = customer_df.dropna("any").select(
        "firstname",
        "lastname",
        string_concat_udf(lit("-"), customer_df.firstname, customer_df['lastname']).alias("Full Name")
    )
    df.show(10)

    print("using udf as Spark SQL function")
    spark.udf.register("string_concat_sql_udf", string_concat,StringType())
    df = customer_df.dropna("any").selectExpr(
        "firstname",
        "lastname",
        "string_concat_sql_udf('->',firstname,lastname) as arrowed_name"
    )
    df.show(10)

    print("using udf in spark sql")

    # customer_df.dropna("any").registerTempTable("customer_df")
    customer_df.dropna("any").createOrReplaceTempView("customer_df")

    query = """select firstname,
                    lastname,
                    string_concat_sql_udf('->',firstname,lastname) as arrowed_name
                from customer_df limit 10
            """
    df = spark.sql(sqlQuery=query)
    df.show()

    print("Saving as table and applying udf in spark sql")
    customer_df.dropna("any").write.saveAsTable("customer_table")
    query = """select firstname,
                    lastname,
                    string_concat_sql_udf('->',firstname,lastname) as arrowed_name
                from customer_table limit 10
            """
    df = spark.sql(sqlQuery=query)
    df.show()











try:
    with utils.spark as spark:
        customer_df: DataFrame = create_dataframes.get_customer_df(spark=spark)
        user_functions()

except Exception as error:
    raise error
