package mllib.classification

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object IrisRandomForest {

  def main(args: Array[String]) {
    run(args(0))
  }

  def run(inputFile: String) {

    // set up
    val conf = new SparkConf().setAppName("Iris RandomForest Classification")
    val sc = new SparkContext(conf)

    // load data
    val data = MLUtils.loadLibSVMFile(sc, inputFile)

    // split data
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // train
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(
      trainingData,
      numClasses,
      categoricalFeaturesInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins)

    // predict
    val labelAndPreds = testData.map { point =>
      (point.label, model.predict(point.features))
    }

    // output
    val testDataCount = testData.count()
    val testSuccessCount = labelAndPreds.filter(r => r._1 == r._2).count()
    val testSuccessRate = testSuccessCount.toDouble / testDataCount
    val testErrorCount = labelAndPreds.filter(r => r._1 != r._2).count()
    val testErrorRate = testErrorCount.toDouble / testDataCount

    println(f"Count(all/success/error): $testDataCount/$testSuccessCount/$testErrorCount")
    println(f"Rate(success/error): $testSuccessRate%1.3f/$testErrorRate%1.3f")
    println(f"Forest model:\n${model.toDebugString}")

    // clean up
    sc.stop()
  }
}
