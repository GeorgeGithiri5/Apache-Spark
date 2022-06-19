from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
        .appName("Python Spark Sql basic example") \
            .config("spark.jars", "/path_to_postgresDriver") \
                .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/databasename") \
    .option("dbtable", "tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()