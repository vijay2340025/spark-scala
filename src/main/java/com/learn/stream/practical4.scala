package com.learn.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object practical4 {
    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("Structured Streaming\"")
          .getOrCreate()


        // 1. read stream
        val df = spark
          .readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", "3131")
          .load()

        // 2. processing
        df.printSchema()
        val resultDF: DataFrame = df.select(
            explode(split(col("value"), " "))
              .as("result")
        ).groupBy("result").count()

        // 3. write to sink
        val dfQuery = resultDF
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()

        dfQuery.awaitTermination()
        spark.close()
    }
}
