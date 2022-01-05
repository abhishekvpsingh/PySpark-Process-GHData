from pyspark.sql.functions import year, month, dayofyear


def df_transform(df):
    return df.withColumn('year', year('created_at')). \
        withColumn('month', month('created_at')). \
        withColumn('day', dayofyear('created_at'))