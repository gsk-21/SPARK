from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit

from utils import spark, users_df

df = users_df.selectExpr('id', 'amount_paid + 50')
df.show()

df = users_df.select(
    'id',
    'amount_paid',
    'amount_paid' + lit(25),
    users_df['amount_paid'] + lit(25),
    (col('amount_paid') + lit(25)).alias('total_amt'),
)

df.show()
