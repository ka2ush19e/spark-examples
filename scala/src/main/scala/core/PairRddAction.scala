package core

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object PairRddAction {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Pair RDD Action")
    val sc = new SparkContext(conf)

    val pairs = sc.parallelize(Array((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"))).persist()

    println("### keys ###")
    println(pairs.keys.collect().mkString(", "))
    println()

    println("### values ###")
    println(pairs.values.collect().mkString(", "))
    println()

    println("### countByKey ###")
    pairs.countByKey().toSeq.sortBy(_._1).foreach { case (n, c) => println(s"$n: $c") }
    println()

    println("### collectAsMap ###")
    pairs.collectAsMap().toSeq.sortBy(_._1).foreach { case (n, s) => println(s"$n: $s") }
    println()

    println("### lookup ###")
    println(s"""1: ${pairs.lookup(1).mkString(", ")}""")
    println(s"""5: ${pairs.lookup(5).mkString(", ")}""")
    println()

    sc.stop()
  }
}
