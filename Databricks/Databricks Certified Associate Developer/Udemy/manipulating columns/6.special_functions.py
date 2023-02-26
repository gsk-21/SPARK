from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, lower, upper

from utils import spark, users_df, employee_df, dummy_df


# 1.col

def col_fn():
    employee_df.orderBy('id').show()

    employee_df.groupBy('id').count().show()

    employee_df.select(
        'ID',
        upper('nationality')
    ).show()

    employee_df.select(
        upper(col('first_name')),
        lower(col('last_name'))
    ).show()

    print("Group by")

    employee_df.groupBy(
        upper('nationality')
    ).count().show()

    print("Order by")

    employee_df.orderBy(
        upper('nationality')
    ).show()

    employee_df.orderBy(
        col('nationality').desc()
    ).show()


# 2.lit
def lit_fns():
    employee_df.select(
        'id',
        concat('first_name', lit('-'), 'salary')
    ).show()

    employee_df.select(
        'id',
        concat(col('first_name'), lit('-'), col('salary'))
    ).show()

    employee_df.withColumn(
        'Bonus', col('salary') + (col('salary') * lit(0.2))
    ).show()

    employee_df.withColumn(
        'Salary_with_Bonus', col('salary') + (col('salary') * lit(0.2))
    ).withColumn(
        'Bonus', col('Salary_with_Bonus') - col('salary')
    ).show()


lit_fns()
