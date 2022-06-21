from ast import alias
import os, datetime
from select import select
from databricks import koalas as ks
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit
from pyspark.sql import SparkSession


def apply_transforms(df: DataFrame)->DataFrame:
    split_col = split(df['_c0'], '\t')

    return df \
        .withColumn("population", split_col.getItem(2).cast('float')) \ 
        .groupBy("country") \
        .agg(col("country"), sum("population")).select(col("country"), col("sum(population)")) \
        .alias("population")

if __name__ == "__main__":