from .main import filter_spark_data_frame
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

def test_filter_spark_data_frame_by_value():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    input = sql_context.createDataFrame(
        [('charly', 15),
        ('fabien', 18),
        ('sam', 21),
        ('sam', 25),
        ('nick', 19),
        ('nick', 40)],
        ['name', 'age']
    )

    expected_output = sql_context.createDataFrame(
        [('sam', 25),
        ('sam', 21),
        ('nick', 40)],
        ['name', 'age']
    )
    real_output = filter_spark_data_frame(
        expected_output.toPandas(),
        ['age', 'name'],
    )

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like = True
    )

    spark_context.stop()

def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)