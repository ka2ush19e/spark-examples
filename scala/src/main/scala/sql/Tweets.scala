package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Tweets {

  def main(args: Array[String]) {
    run(args(0))
  }

  def run(inputFile: String) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Load JSON")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)

    val inputs = hiveCtx.jsonFile(inputFile)
    inputs.registerTempTable("tweets")

    println("### Schema ###")
    inputs.printSchema()
  }
}
