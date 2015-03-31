package mllib.recommendation

import java.io.File

import scala.io.Source

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object MovieLensALS {

  def main(args: Array[String]) {
    run(args(0), args(1), args(2))
  }

  def run(movieLensHomeDir: String, myRatingsFile: String, executeType: String) {

    // set up
    val conf = new SparkConf().setAppName("MovieLens ALS Recommendation")
    val sc = new SparkContext(conf)

    // load personal ratings
    val myRatings = {
      Source.fromFile(myRatingsFile).getLines().map { line =>
        val fields = line.split("::")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }.filter(_.rating > 0.0).toSeq
    }
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    // load ratings
    val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    // load movies
    val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect().toMap

    // output ratings
    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // split training data
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    executeType match {
      case "evaluate" => evaluate(training, validation, test)
      case "recommend" => recommend(sc, training, myRatings, movies)
    }

    sc.stop()
  }

  def evaluate(training: RDD[Rating], validation: RDD[Rating], test: RDD[Rating]) {

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println(s"Training: $numTraining, validation: $numValidation, test: $numTest")

    // training
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)

      println(s"RMSE (validation) = $validationRmse for the model trained " +
        s"with rank = $rank, lambda = $lambda, and numIter = $numIter.")

      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    val testRmse = computeRmse(bestModel.get, test, numTest)

    println(s"The best model was trained " +
      s"with rank = $bestRank and lambda = $bestLambda, and numIter = $bestNumIter, " +
      s"and its RMSE on the test set is $testRmse.")

    val meanRating = training.union(validation).map(_.rating).mean()
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean())
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println(f"The best model improves the baseline by $improvement%1.2f%%.")
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  def recommend(
    sc: SparkContext,
    training: RDD[Rating],
    myRatings: Seq[Rating],
    movies: Map[Int, String]) {
    val model = ALS.train(training, 12, 10, 0.1)

    val myRatedMovieIds = myRatings.map(_.product)
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = model
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(-_.rating)
      .take(50)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println(f"  $i%2d: ${movies(r.product)}%s")
      i += 1
    }
  }
}
