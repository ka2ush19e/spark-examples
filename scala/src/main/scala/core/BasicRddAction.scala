package core

import org.apache.spark.{SparkConf, SparkContext}

object BasicRddAction {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Basic RDD Action")
    val sc = new SparkContext(conf)

    case class Str(s: String)

    class StrOrdering extends Ordering[Str] {
      override def compare(x: Str, y: Str): Int = x.s compare y.s
    }

    val nums = sc.parallelize(Array(1, 1, 1, 2, 3, 3, 4, 4, 4, 5, 5, 5)).persist()
    val strs = sc.parallelize(Array(Str("a"), Str("b"), Str("c"), Str("d"), Str("e"))).persist()

    println("### first ###")
    println(nums.first())
    println()

    println("### collect ###")
    println(nums.collect().mkString(", "))
    println()

    println("### count ###")
    println(nums.count())
    println()

    println("### countByValue ###")
    nums.countByValue().toSeq.sortBy(_._1).foreach { case (n, c) => println(s"$n: $c") }
    println()

    println("### take ###")
    println(nums.take(5).mkString(", "))
    println()

    println("### top ###")
    println(nums.top(4).mkString(", "))
    println(strs.top(4)(new StrOrdering).mkString(", "))
    println()

    println("### takeOrdered ###")
    println(nums.takeOrdered(5).mkString(", "))
    println(strs.takeOrdered(5)(new StrOrdering).mkString(", "))
    println()

    println("### takeSample ###")
    println(nums.takeSample(false, 3).mkString(", "))
    println()

    println("### reduce ###")
    println(nums.reduce(_ + _))
    println()

    println("### fold ###")
    println(nums.fold(0)(_ + _))
    println()

    println("### aggregate ###")
    println(nums.aggregate(0)(_ + _, _ + _))
    println()

    println("### foreach ###")
    nums.foreach(n => println(n))
    println()

    println("### foreachPartition ###")
    nums.foreachPartition(p => println(p.toSeq.mkString(", ")))
    println()

    sc.stop()
  }
}
