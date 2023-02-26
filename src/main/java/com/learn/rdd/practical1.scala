package com.learn.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object practical1 {
    Logger.getLogger("org").setLevel(Level.INFO)
    private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practical1")
    private val sc: SparkContext = new SparkContext(config = conf)

    def main(args: Array[String]): Unit = {
        print("Hello World !!!")

        val list = List(1, 2, 3)

        val rdd = sc.parallelize(list)

        rdd.map(x => x * x)
          .foreach(println)
    }
}
