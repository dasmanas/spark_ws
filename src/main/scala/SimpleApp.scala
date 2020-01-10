import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/usr/local/Cellar/apache-spark/2.4.4/README.md" // Should be some file on your system
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SimpleSparkApp")
    val spark = SparkSession.builder.appName("Simple Application")
      .config(conf)
      .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}


/**
 * spark-submit \
 * --class "SimpleApp" \
 * --master "local[*]" \
 * target/scala-2.11/spark_ws_2.11-0.1.jar
 */
