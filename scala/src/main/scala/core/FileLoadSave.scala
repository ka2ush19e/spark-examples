package core

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{KeyValueTextInputFormat, TextOutputFormat}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object FileLoadSave {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("File Load Save")
    val sc = new SparkContext(conf)

    val saveData = sc.parallelize(Array((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e")))

    println("### SequenceFile ###")
    saveData.saveAsSequenceFile("output_sequence")

    val loadDataFromSequenceFile = sc.sequenceFile("output_sequence", classOf[IntWritable], classOf[Text]).map {
      case (n, s) => (n.get(), s.toString)
    }
    loadDataFromSequenceFile.collect().foreach { case (n, s) => println(s"$n: $s") }
    println()

    println("### ObjectFile ###")
    saveData.saveAsObjectFile("output_object")

    val loadDataFromObjectFile = sc.objectFile[(Int, String)]("output_object")
    loadDataFromObjectFile.collect().foreach { case (n, s) => println(s"$n: $s") }
    println()

    println("### HadoopFile ###")
    saveData.saveAsHadoopFile(
      "output_hadoop",
      classOf[IntWritable],
      classOf[Text],
      classOf[TextOutputFormat[IntWritable, Text]])

    val loadDataFromHadoopFile =
      sc.hadoopFile[Text, Text, KeyValueTextInputFormat]("output_hadoop").map { case (k, v) =>
        (k.toString.toInt, v.toString)
      }
    loadDataFromHadoopFile.collect().foreach { case (n, s) => println(s"$n: $s") }
    println()

    println("### Compressed HadoopFile ###")
    saveData.saveAsHadoopFile(
      "output_hadoop_compressed",
      classOf[IntWritable],
      classOf[Text],
      classOf[TextOutputFormat[IntWritable, Text]],
      classOf[GzipCodec])

    val loadDataFromCompressedHadoopFile =
      sc.hadoopFile[Text, Text, KeyValueTextInputFormat]("output_hadoop_compressed").map { case (k, v) =>
        (k.toString.toInt, v.toString)
      }
    loadDataFromCompressedHadoopFile.collect().foreach { case (n, s) => println(s"$n: $s") }

    sc.stop()
  }
}
