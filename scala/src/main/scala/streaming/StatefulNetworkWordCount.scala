package streaming

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object StatefulNetworkWordCount {

  def main(args: Array[String]) {
    run(args(0), args(1).toInt)
  }

  def run(hostname: String, port: Int): Unit = {
    val ssc = StreamingContext.getOrCreate("checkpoint", () => {
      val conf = new SparkConf().setAppName("Stateful Network Word Count")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val ssc_ = new StreamingContext(conf, Seconds(5))
      ssc_.checkpoint("checkpoint")

      val lines = ssc_.socketTextStream(hostname, port, StorageLevel.MEMORY_AND_DISK_SER)
      val wordCounts = lines.flatMap(_.split(" "))
        .map(_.toLowerCase)
        .map((_, 1L))
        .updateStateByKey(
          (values: Seq[Long], state: Option[Long]) => {
            Some(values.sum + state.getOrElse(0L))
          }
        )

      wordCounts.print()
      ssc_
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
