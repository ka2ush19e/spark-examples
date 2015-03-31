package mllib.clustering

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object IrisKMeans {

  def main(args: Array[String]) {
    run(args(0))
  }

  def run(inputFile: String) {

    // set up
    val conf = new SparkConf().setAppName("Iris KMeans Clustering")
    val sc = new SparkContext(conf)

    // load data
    val data = sc.textFile(inputFile).filter(_.nonEmpty).map { line =>
      val fields = line.split(",")
      (fields.last, Vectors.dense(fields.init.map(_.toDouble)))
    }

    // clustering
    val numClusters = 3
    val numIterations = 10
    val clusters = KMeans.train(data.map(_._2), numClusters, numIterations)

    // evaluate
    val WSSSE = clusters.computeCost(data.map(_._2))
    println(s"Within Set Sum of Squared Errors: $WSSSE")

    data.sortBy(_._1).foreach {
      case (label, feature) =>
        val custerNo = clusters.predict(feature)
        println(f"${feature.toArray.mkString("[", ", ", "]")} $label%-15s => $custerNo")
    }

    // clean up
    sc.stop()
  }
}
