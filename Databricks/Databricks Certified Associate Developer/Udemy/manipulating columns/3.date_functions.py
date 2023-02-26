from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, lower, upper, ltrim, rtrim

from utils import spark, users_df, employee_df, dummy_df, employee_df_phones


def date_timestamp_basics():
    dummy_df.select(
        current_date(),
        current_timestamp()
    ).show(truncate=False)

    df = dummy_df.select(
        to_date(lit('20230225'), 'yyyyMMdd').alias('date_1'),
        to_date(lit('20232602'), 'yyyyddMM').alias('date_2'),
        to_date(lit('26022023'), 'ddMMyyyy').alias('date_3'),
        to_timestamp(lit('26022023 0125'), 'ddMMyyyy HHmm').alias('ts_1'),
        to_timestamp(lit('26022023 1525'), 'ddMMyyyy HHmm').alias('ts_2'),
        to_timestamp(lit('26022023 1125'), 'ddMMyyyy hhmm').alias('ts_3'),
    )

    df.show(truncate=False)

    dates = [
        ("2022-02-28", "2022-02-28 10:00:00.123"),
        ("2016-02-29", "2016-02-29 08:08:08.999"),
        ("2017-10-31", "2017-10-31 11:59:59.123"),
        ("2019-11-30", "2019-11-30 00:00:00.000")
    ]

    dates_df = spark.createDataFrame(
        data=dates,
        schema="""
            date STRING,
            time STRING
        """
    )

    print("Add, Sub 10")

    dates_df.select(
        'date',
        date_add('date', 10).alias('date + 10'),
        date_sub('date', 10).alias('date - 10'),
        'time',
        date_add('time', 10).alias('time + 10'),
        date_sub('time', 10).alias('time - 10')
    ).show()

    print("Date Difference")

    dates_df.select(
        'date',
        datediff(current_date(), 'date').alias('date_difference'),
        'time',
        datediff(current_timestamp(), 'time', ).alias('time_difference')
    ).show()

    print("Month difference")

    dates_df.select(
        'date',
        round(months_between('date', current_date())).alias('months_between-date'),
        round(months_between(current_date(), 'date'), 2).alias('months_between-date'),
        add_months('date', 3).alias('date + 3months'),
        'time',
        months_between(current_timestamp(), 'time', ).alias('months_between-time'),
        add_months('time', 3).alias('time + 3months'),
    ).show()

    print('trunc - Returns first day of Month, year, week, quarter')

    dates_df.select(
        'date',
        trunc('date', 'year').alias('year-first-day'),
        trunc('date', 'MM').alias('month-first-day'),
        trunc('date', 'week').alias('week-first-day'),
        trunc('date', 'quarter').alias('quater-first-day'),

        'time',
        trunc('time', 'year').alias('time-year-first-day'),
        trunc('time', 'MM').alias('time-month-first-day'),
        trunc('time', 'week').alias('time-week-first-day'),
        trunc('time', 'quarter').alias('time-quater-first-day')
    ).show(truncate=False)

    print('date_trunc - Returns first day of Month, year, week, quarter')

    dates_df.select(
        'date',
        date_trunc('year', 'date').alias('year-first-day'),
        date_trunc('MM', 'date').alias('month-first-day'),
        date_trunc('week', 'date').alias('week-first-day'),
        date_trunc('quarter', 'date').alias('quater-first-day'),
        date_trunc('HOUR', 'date').alias('first-hour'),
        'time',
        date_trunc('HOUR', 'time').alias('time-first-hour'),
        date_trunc('second', 'time').alias('time-first-second'),

    ).show(truncate=False)


def date_and_time_extract():
    df = dummy_df

    print("Date")

    df.select(
        current_date(),
        year(current_date()).alias('year'),
        month(current_date()).alias('month'),
        weekofyear(current_date()).alias('weekofyear'),
        dayofweek(current_date()).alias('dayofweek'),
        dayofyear(current_date()).alias('dayofyear'),
        dayofmonth(current_date()).alias('dayofmonth'),

        hour(current_date()).alias('hour'),
        minute(current_date()).alias('minute'),
        second(current_date()).alias('second'),
    ).show()

    print("Timestamp")

    df.select(
        current_timestamp(),
        year(current_timestamp()).alias('year'),
        month(current_timestamp()).alias('month'),
        weekofyear(current_timestamp()).alias('weekofyear'),
        dayofweek(current_timestamp()).alias('dayofweek'),
        dayofyear(current_timestamp()).alias('dayofyear'),
        dayofmonth(current_timestamp()).alias('dayofmonth'),

        hour(current_timestamp()).alias('hour'),
        minute(current_timestamp()).alias('minute'),
        second(current_timestamp()).alias('second'),
    ).show(truncate=False)


def to_date_timestamp():
    df = dummy_df

    df.select(
        lit('21 Mar 2023'),
        lit('21 March 2023'),
        lit('21 Mar 2023 10:00:00.123'),
        lit('21 March 2023 10:25:00')
    ).show(truncate=False)

    df.select(
        to_date(lit('21 Mar 2023'), 'dd MMM yyyy'),
        to_date(lit('21 March 2023'), 'dd MMMM yyyy'),
        to_timestamp(lit('21 Mar 2023 10:00:00.123'), 'dd MMM yyyy HH:mm:ss.SSS'),
        to_timestamp(lit('21 March 2023 10:25:00'), 'dd MMMM yyyy HH:mm:ss'),
    ).show(truncate=False)


def date_format_excercise():
    dates = [
        ("2022-02-28", "2022-02-28 10:00:00.123"),
        ("2016-02-29", "2016-02-29 08:08:08.999"),
        ("2017-10-31", "2017-10-31 11:59:59.123"),
        ("2019-11-30", "2019-11-30 00:00:00.000")
    ]

    dates_df = spark.createDataFrame(
        data=dates,
        schema="""
            date STRING,
            time STRING
        """
    )

    dates_df.select(
        'date',
        date_format('date', 'yyyyMM').alias('formatted_date'),
        'time',
        date_format('time', 'yyyyMM').alias('formatted_time'),
        date_format('time', 'yyyyMMddHHmmss').alias('formatted_timestamp'),
        date_format('time', 'yyyyDDD').alias('time_yd'),
    ).show()

    dates_df.select(
        'date',

        date_format('date', 'MMM d, yyyy').alias('date_desc'),
        date_format('date', 'MMMM d, yyyy').alias('date_desc_full'),
        date_format('date', 'EE').alias('day'),
        date_format('date', 'EEEE').alias('day_full'),

    ).show()


def unix():
    dates = [
        ("20220228", "2022-02-28", "2022-02-28 10:00:00"),
        ("20160229", "2016-02-29", "2016-02-29 08:08:08"),
        ("20171031", "2017-10-31", "2017-10-31 11:59:59"),
        ("20191130", "2019-11-30", "2019-11-30 00:00:00")
    ]

    unix_timestamps = [
        ('1645986600',),
        ('1456684200',),
        ('1509388200',),
        ('1575052200',)
    ]

    dates_df = spark.createDataFrame(
        data=dates,
        schema="""
            dateid STRING,
            date STRING,
            time STRING
        """
    )

    unix_df = spark.createDataFrame(
        data=unix_timestamps,
        schema="unixtime STRING"
    )

    dates_df.select(
        'dateid',
        unix_timestamp('dateid', 'yyyyMMdd').alias('dateid-unix'),
        'date',
        unix_timestamp('date', 'yyyy-MM-dd').alias('date-unix'),
        'time',
        unix_timestamp('time').alias('time-unix'),
    ).show()

    unix_df.select(
        'unixtime',
        from_unixtime('unixtime').alias("default"),
        from_unixtime('unixtime', 'yyyy-MM-dd').alias("default"),
        col('unixtime').cast('int').cast('timestamp')
    ).show()


# date_timestamp_basics()
# date_and_time_extract()
# to_date_timestamp()
# date_format_excercise()
unix()
