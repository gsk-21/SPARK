from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit

from utils import spark, users_df as df

df.select(
    'id',
    'first_name',
    'last_name'
).withColumn(
    'full_name', concat('first_name', lit(' '), 'last_name')
).show()

df.select(
    'id',
    'first_name'
).withColumnRenamed('first_name', 'fn').show()

df.withColumn(
    'total_courses', size('courses')
).select(
    'id',
    'courses',
    'total_courses'
).show()

print("Renaming using alias......")

df.select(
    col('id').alias('user_id'),
    col('first_name').alias('first name'),
    col('last_name').alias('last name'),
    (concat(col('first_name'), lit(' '), col('last_name')))
).show()

df.select(
    df['id'].alias('user_id'),
    df['first_name'].alias('first name'),
    df['last_name'].alias('last name'),
    (concat(col('first_name'), lit(' '), col('last_name'))).alias('full_name')
).show()

df.select(
    col('id').alias('user_id'),
    col('first_name').alias('first name'),
    col('last_name').alias('last name'),
).withColumn(
    'full_name',
    (concat(col('first name'), lit(' '), col('last name')))
).show()

df.withColumnRenamed('id', 'user_id').select('*').show()


print("Renaming all the columns using list")
required_cols = ['id', 'first_name', 'last_name']
renamed_cols = ['user_id', 'fn', 'ln']

df.select(required_cols).toDF(*renamed_cols).show()



