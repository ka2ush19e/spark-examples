package core

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("GroupBy")
    val sc = new SparkContext(conf)

    val nums = sc.parallelize(0 until 5, 5).flatMap { _ =>
      val rnd = new Random
      val buffer = ListBuffer[Int]()
      for (i <- 0 until 1000) {
        buffer += rnd.nextInt(10)
      }
      buffer.toList
    }.persist()

    println("### Count ###")
    println(nums.count())
    println()

    println("### Average: reduce ###")
    val avg1 = nums.map((_, 1)).reduce { case (x, y) => (x._1 + y._1, x._2 + y._2) }
    println(avg1._1 * 1.0 / avg1._2)
    println()

    println("### Average: fold ###")
    val avg2 = nums.map((_, 1)).fold((0, 0)) { case (x, y) => (x._1 + y._1, x._2 + y._2) }
    println(avg2._1 * 1.0 / avg2._2)
    println()

    println("### Average: aggregate ###")
    val avg3 = nums.aggregate((0, 0))(
      (acc, v) => (acc._1 + v, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    println(avg3._1 * 1.0 / avg3._2)
    println()

    println("### Grouping: reduceByKey ###")
    nums.map((_, 1)).reduceByKey(_ + _).sortByKey().collect().foreach(println)
    println()

    println("### Grouping: foldByKey ###")
    nums.map((_, 1)).foldByKey(0)(_ + _).sortByKey().collect().foreach(println)
    println()

    println("### Grouping: aggregateByKey ###")
    nums.map((_, 1)).aggregateByKey(0)(
      (acc, v) => acc + v,
      (acc1, acc2) => acc1 + acc2
    ).sortByKey().collect().foreach(println)
    println()

    println("### Grouping: combineByKey ###")
    nums.map((_, 1)).combineByKey(
      (v: Int) => v,
      (acc: Int, v: Int) => acc + v,
      (acc1: Int, acc2: Int) => acc1 + acc2
    ).sortByKey().collect().foreach(println)
    println()

    sc.stop()
  }
}
