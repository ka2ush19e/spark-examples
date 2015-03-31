package mllib

import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

object FeatureExtraction {
  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("TFIDF")
    val sc = new SparkContext(conf)

    val documents = sc.parallelize(Seq(
      "hello world",
      "hello spark",
      "hello scala",
      "hello mllib",
      "goodbye mapreduce"
    ))

    val tf = new HashingTF(10)
    val tfVectors = documents.map(d => tf.transform(d.split(" "))).persist()

    println("### TF ###")
    tfVectors.collect().foreach(println)
    println()

    println("### TFIDF ###")
    val idf = new IDF().fit(tfVectors)
    val tfIdfVectors = idf.transform(tfVectors)
    tfIdfVectors.collect().foreach(println)
    println()

    println("### Standard scale ###")
    val scaler = new StandardScaler(true, true).fit(tfVectors.map(v => Vectors.dense(v.toArray)))
    scaler.transform(tfVectors.map(v => Vectors.dense(v.toArray))).collect().foreach(println)
    println()

    println("### Normalize ###")
    val normalizer = new Normalizer()
    tfVectors.map(normalizer.transform).collect().foreach(println)
    println()

    println("### Stats ###")
    val stats = Statistics.colStats(tfIdfVectors)
    println(stats.count)
    println(stats.mean)
    println(stats.variance)
    println(stats.numNonzeros)
    println()

    println("### PCA ###")
    val mat = new RowMatrix(tfIdfVectors)
    val pc = mat.computePrincipalComponents(2)
    println(pc)
    println()

    mat.multiply(pc).rows.collect().foreach(println)
    println()

    println("### SVD ###")
    val svd = mat.computeSVD(2, computeU = true)
    svd.U.rows.collect().foreach(println)
    println()
    println(svd.s)
    println()
    println(svd.V)
    println()
  }
}
