package core

import org.apache.spark.{SparkConf, SparkContext}

object SetOperation {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Set Operation")
    val sc = new SparkContext(conf)

    val nums1 = sc.parallelize(Array(5, 5, 4, 4, 3, 3, 2, 2, 1, 1))
    val nums2 = sc.parallelize(Array(5, 3, 3, 1, 1))

    println("### Distinct ###")
    println(nums1.distinct().collect().mkString(","))
    println()

    println("### Union ###")
    println(nums1.union(nums2).collect().mkString(","))
    println()

    println("### Intersection ###")
    println(nums1.intersection(nums2).collect().mkString(","))
    println()

    println("### Subtract ###")
    println(nums1.subtract(nums2).collect().mkString(","))
    println()

    println("### Cartesian ###")
    println(nums1.cartesian(nums2).collect().mkString(","))
    println()
  }
}
