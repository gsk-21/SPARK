from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, lower, upper, ltrim, rtrim

from utils import spark, users_df, employee_df, dummy_df, employee_df_phones


# 1. Case Conversion      - lower, upper
# 2. Getting Length       - len
# 3. Concatenating string - concat, concat_ws

def exercises_1():
    employee_df.select(
        '*',
        lower('nationality').alias('nationality')
    ).show()

    employee_df.select(
        '*',
        lower('nationality').alias('nationality')
    ).printSchema()

    print("WithColumn")

    employee_df.withColumn(
        'nationality', lower('nationality')
    ).show()

    print('\nconcat')
    employee_df.select(
        'first_name',
        'last_name',
        concat('first_name', lit(','), 'last_name', )
    ).show()

    print('\nconcat_ws')
    employee_df.select(
        'first_name',
        'last_name',
        concat_ws(',', 'first_name', 'last_name', )
    ).show()

    print("upper, lower, initcap, length")
    employee_df.select(
        'id',
        'nationality'
    ).withColumn(
        'nationality_upper',
        upper('nationality')
    ).withColumn(
        'nationality_lower',
        lower('nationality')
    ).withColumn(
        'nationality_title',
        initcap('nationality_lower')
    ).withColumn(
        'nationality_length',
        length('nationality')
    ).show()


# 4. Extracting substring - split, substring
def substring_exercises():
    dummy_df.select(
        substring(lit('Hello World'), 1, 5)
    ).show()

    dummy_df.select(
        substring(lit('Hello World'), -5, 5)
    ).show()

    df = employee_df.withColumn(
        'name', concat('first_name', lit(' '), 'last_name')
    ).select(
        ['name', 'ssn', 'phone_number']
    )

    print("Employee DF")
    df.show()

    print("Last 4 digits")
    df = df.withColumn(
        'last_4_phone', substring('phone_number', -4, 4)
    ).withColumn(
        'last_4_ssn', substring('ssn', 8, 4)
    )

    df.show()


def split_exercises():
    dummy_df.select(
        split(lit('Hello World, How are you?'), ' ')
    ).show()

    dummy_df.select(
        split(lit('Hello World, How are you?'), ' ', 2)
    ).show()

    dummy_df.select(
        explode(
            split(lit('Hello World, How are you?'), ' ')
        )
    ).show()

    df = employee_df_phones.withColumn(
        'name', concat('first_name', lit(' '), 'last_name')
    ).select(
        ['name', 'ssn', 'phone_numbers']
    )

    df.show(truncate=False)

    df = df.withColumn(
        'phone_number', explode(split(col('phone_numbers'), ','))
    ).drop('phone_numbers')

    df.show(truncate=False)

    df = df.withColumn(
        'area_code', split(col('phone_number'), ' ')[1]
    ).withColumn(
        'phone_last4', split(col('phone_number'), ' ')[3]
    ).withColumn(
        'ssn_last4', split(col('ssn'), ' ')[2]
    )

    df.show(truncate=False)

    print("Phone Count")
    df.groupBy('name').count().show()


# 5. Padding              - lpad, rpad
def padding_exercises():
    dummy_df.select(
        rpad(lit("Hello"), 10, '-'),
        lpad(lit("Hello"), 10, '-'),
    ).show()

    # id
    # first_name
    # last_name
    # salary
    # nationality
    # phone_number
    # ssn

    df = employee_df.withColumn(
        'id', lpad('id', 5, '0')
    ).withColumn(
        'first_name', rpad('first_name', 10, '-')
    ).withColumn(
        'last_name', rpad('last_name', 10, '-')
    ).withColumn(
        'salary', lpad('salary', 10, '0')
    ).withColumn(
        'nationality', rpad('nationality', 15, '-')
    ).withColumn(
        'phone_number', rpad('phone_number', 17, '-')
    )

    df.show(truncate=False)

    empFixedDF = df.select(
        concat(
            'id',
            'first_name',
            'last_name',
            'salary',
            'nationality',
            'phone_number',
            'ssn'
        )
    )

    empFixedDF.show(truncate=False)


# 6. Trimming             - ltrim, rtrim, trim
def trimming_exercises():
    df = dummy_df.withColumn(
        'x', lit('..Hello...')
    )

    df.show()

    df.select(
        'x',
        ltrim('x'),
        rtrim('x'),
        expr('trim(x)'),
        expr('trim(BOTH from x)'),
        expr('trim(LEADING from x)'),
        expr('trim(TRAILING from x)'),
    ).show()

    df.select(
        'x',
    ).withColumn(
        'trim', expr("trim(BOTH '.' from x)")
    ).withColumn(
        'ltrim', expr("trim(LEADING '.' from x)"),
    ).withColumn(
        'rtrim', expr("trim(TRAILING '.' from x)")
    ).select('*').show()


# exercises_1()
# substring_exercises()
# split_exercises()
# padding_exercises()
trimming_exercises()
