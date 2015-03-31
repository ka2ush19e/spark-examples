package streaming

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object NetworkWordCount {

  def main(args: Array[String]) {
    run(args(0), args(1).toInt)
  }

  def run(hostname: String, port: Int): Unit = {
    val conf = new SparkConf().setAppName("Network Word Count")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_AND_DISK_SER)
    val wordCounts = lines.flatMap(_.split(" "))
      .map(_.toLowerCase)
      .map((_, 1L))
      .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
