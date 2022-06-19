from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.14.jar") \
    .getOrCreate()

jbdcDF = spark.read.format("jdbc") \
    options(
        url = 'jdbc:postgresql://localhost:5432/database_example',
        dbtable = 'Employee',
        user = 'postgres',
        password = '1234',
        driver = 'org.postgresql.Driver').\
    load()

jbdcDF.printSchema()
jbdcDF.select('*').collect()