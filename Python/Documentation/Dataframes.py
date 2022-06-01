from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1',d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2',d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3',d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

df1 = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')

df

pandas_df = pd.DataFrame({
    'a':[1, 2, 3],
    'b':[2., 3., 4.],
    'c':['string1', 'string2', 'string3'],
    'd':[date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})

df=spark.createDataFrame(pandas_df)

rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])

df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
df 

df.show()
df.printSchema()

df.show(1)

# Show Summary of dataframe
df.select('a', 'b', 'c').describe().show()

df.collect()

df.take()
df.tail()

df.select(df.c).show()

from pyspark.sql.functions import upper
df.withColumn('upper_c', upper(df.c)).show()

df.filter(df.a==1).show()

df.groupby('color').avg().show()