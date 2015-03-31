package mllib.classification

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EnronLogisticRegression {

  def main(args: Array[String]) {
    run(args(0), args(1))
  }

  def run(trainingDataDir: String, testDataDir: String) {

    // set up
    val conf = new SparkConf().setAppName("Enron LogisticRegression Classification")
    val sc = new SparkContext(conf)

    // load data
    val tf = new HashingTF(10000)

    def loadData(dataDir: String, dirnameFilter: String, label: Int): RDD[LabeledPoint] = {
      sc.wholeTextFiles(dataDir).filter(_._1.contains(dirnameFilter)).map {
        case (path, email) =>
          LabeledPoint(label, tf.transform(email.split(" ")))
      }
    }

    val trainingSpamData = loadData(trainingDataDir, "spam", 1)
    val trainingHamData = loadData(trainingDataDir, "ham", 0)
    val trainingData = trainingSpamData.union(trainingHamData).persist()

    // train
    val model = new LogisticRegressionWithSGD().run(trainingData)

    // predict
    val testSpamData = loadData(testDataDir, "spam", 1)
    val testHamData = loadData(testDataDir, "ham", 0)
    val testData = testSpamData.union(testHamData)

    val preds = model.predict(testData.map(_.features))
    val predAndLabels = preds.zip(testData.map(_.label))

    // output
    val testDataCount = testData.count()
    val testSuccessCount = predAndLabels.filter(r => r._1 == r._2).count()
    val testSuccessRate = testSuccessCount.toDouble / testDataCount
    val testErrorCount = predAndLabels.filter(r => r._1 != r._2).count()
    val testErrorRate = testErrorCount.toDouble / testDataCount

    println(f"Count(all/success/error): $testDataCount/$testSuccessCount/$testErrorCount")
    println(f"Rate(success/error): $testSuccessRate%1.3f/$testErrorRate%1.3f")

    val metrics = new BinaryClassificationMetrics(predAndLabels)
    println(s"areaUnderPR: ${metrics.areaUnderPR()}")
    println(s"areaUnderROC: ${metrics.areaUnderROC()}")

    // clean up
    sc.stop()
  }
}
