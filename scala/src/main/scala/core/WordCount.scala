package core

import scala.io.Source

import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    run(args)
  }

  def run(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")

    val stopWords = sc.broadcast(
      Source.fromFile("src/main/resources/stopwords.txt").getLines().toSet)

    val stopWordCount = sc.accumulator(0L)

    val wordCounts = sc.textFile(args(0))
      .flatMap(_.split("[\\s,.:;\'\"?!\\-\\(\\)\\[\\]\\{\\}]"))
      .map(_.toLowerCase)
      .filter(_.length > 1)
      .filter { w =>
      val result = !stopWords.value.contains(w)
      if (!result) stopWordCount.add(1L)
      result
    }
      .map((_, 1L))
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val count = wordCounts.map { case (w, c) => c }.sum().toLong

    println("### All words count ###")
    println(count)
    println()

    println("### Stopped word count ###")
    println(stopWordCount.value)
    println()

    println("### Ranking ###")
    wordCounts.sortBy(_._2, ascending = false).take(10).foreach { v =>
      println(f"${v._1}%-8s ${v._2}%4d")
    }
    println()

    println("### Ranking and rate ###")
    wordCounts.mapValues(v => (v, v * 1.0 / count * 100))
      .sortBy(_._2._1, ascending = false).take(10).foreach { case (k, v) =>
      println(f"$k%-8s ${v._1}%4d ${v._2}%6.2f")
    }
    println()

    sc.stop()
  }
}
