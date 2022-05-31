from pyspark.sql import Row, SparkSession

# row = Row("James", 40)
# Person = Row("name", "age")
# p1=Person("Alice", 40)
# p2=Person("James", 35)
# print(p1.name+","+p2.name)

spark = SparkSession.builder.appName('SparkBy').getOrCreate()

data = [
    Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"), 
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")
]

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())