package com.practice.streaming.discretized

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}


object WindowedSparkStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(4))

    wordCounts.print()

    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
