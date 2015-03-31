package core


import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark._

object Partition {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)

    val nums1 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))).persist()
    val nums2 = nums1.partitionBy(new HashPartitioner(2))
    val nums3 = nums1.partitionBy(new RangePartitioner(3, nums1))
    val nums4 = nums1.partitionBy(new RandomPartitioner(2))

    println("### Default ###")
    nums1.foreachPartition(p => println(p.toSeq.mkString(",")))
    println()

    println("### HashPartitioner ###")
    nums2.foreachPartition(p => println(p.toSeq.mkString(",")))
    println()

    println("### RangePartitioner ###")
    nums3.foreachPartition(p => println(p.toSeq.mkString(",")))
    println()

    println("### RandomPartitioner ###")
    nums4.foreachPartition(p => println(p.toSeq.mkString(",")))
    println()
  }
}

class RandomPartitioner(numParts: Int) extends Partitioner with Serializable {
  val rnd = new Random()

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    rnd.nextInt(Int.MaxValue) % numPartitions
  }

  override def equals(other: Any): Boolean = {
    other match {
      case p: RandomPartitioner => p.numPartitions == numPartitions
      case _ => false
    }
  }
}
