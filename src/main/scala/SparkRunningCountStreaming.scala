import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkRunningCountStreaming {


  def main(args: Array[String]): Unit = {

    val checkpointDirectory = "/Users/manasdas/Downloads/checkpointDirectory"

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("NetworkWordRunningCount")
      val ssc = new StreamingContext(conf, Seconds(5))

      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val words: DStream[String] = lines.flatMap(_.split(" "))
      val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
      val runningCounts: DStream[(String, Int)] = pairs.updateStateByKey(updateFunction _)
      val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
      wordCounts.print()
      runningCounts.print()

      ssc.checkpoint(checkpointDirectory) // set checkpoint directory
      ssc
    }


    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = newValues.sum + runningCount.getOrElse(0)
      Some(newCount)
    }


    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)



    ssc.start()
    ssc.awaitTermination()
  }


}
