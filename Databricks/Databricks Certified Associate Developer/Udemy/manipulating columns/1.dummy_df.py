from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, lower

from utils import spark, users_df, employee_df, dummy_df

print("Dummy DF")
dummy_df.show()
dummy_df.select(
    current_date(),
    date_format(current_date(), 'yyyyMM').alias('year_month')
).show()

print("Employee DF")
employee_df.show()
employee_df.printSchema()

help(lower)
