package com.learn.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object practical1 {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Stream basics")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) // 5 sec batch interval

    def main(args: Array[String]): Unit = {
        val stream = ssc.socketTextStream("localhost", 8989)
        val wordPairs = stream.flatMap(x => x.split(" ")).map(x => (x, 1))
        // calling stateless transformations
        wordPairs.reduceByKey((x, y) => x + y).print()
        // use nc -lk <port>  to capture the network packets over socket
        ssc.start()
        ssc.awaitTermination()
    }

}
