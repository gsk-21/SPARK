from pyspark.sql.functions import *
from pyspark.sql.functions import col

from utils import spark, users_df

df = users_df.select(
    col('id'),
    users_df['customer_from'],
    date_format(col('customer_from'), 'yyyyMMdd').alias('int_date'),
)
df.show()

cols = [
    col('id'),
    users_df['customer_from'],
    date_format(col('customer_from'), 'yyyyMMdd').cast('int').alias('int_date')
]

df.select(cols).show()

df.select(cols).printSchema()

df.select(*cols).show()

df.select(*cols).printSchema()

customer_from_alias = date_format(col('customer_from'), 'yyyyMMdd').cast('int').alias('int_date')

df.select('id', customer_from_alias).show()