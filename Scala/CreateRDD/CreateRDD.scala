package com.sparkbyexamples.spark.rdd 

import org.apache.spark.sql.SparkSession

object CreatedEmptyRDD extends App{
    val spark:SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("SparkByExamples")
        .getOrCreate()
    val rdd = spark.sparkContext.emptyRDD 
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)
    println("Num of Partitions: "+rdd.getNumPartitions)

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of Partitions: "+rdd2.getNumPartitions)

    type dataType = (String, Int)
    var pairRDD = spark.sparkContext.emptyRDD[dataType]
    println(pairRDD)
}