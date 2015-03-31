package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object RestaurantsParquet {

  def main(args: Array[String]) {
    run(args(0))
  }

  def run(inputFile: String) {
    val conf = new SparkConf().setAppName("Restaurants Parquet")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    import hiveCtx._

    val inputs = hiveCtx.parquetFile(inputFile)
    inputs.registerTempTable("restaurants")

    registerFunction("LEN", (s: String) => s.length)
    hiveCtx.sql("CREATE TEMPORARY FUNCTION strLen AS 'sql.StrLen'")

    println("### Schema ###")
    inputs.printSchema()
    println()

    println("### Restaurants in Tokyo ###")
    val restaurantsInTokyo = hiveCtx.sql(
      """SELECT
        |    r.id,
        |    r.alphabet,
        |    LEN(r.alphabet),
        |    strLen(r.alphabet)
        |FROM
        |    restaurants r
        |WHERE
        |    r.pref_id = '13'
        |AND r.alphabet <> ''
        |LIMIT
        |    10
      """.stripMargin)

    restaurantsInTokyo.collect().foreach { row =>
      println(f"${row.getString(0)}%3s ${row.getString(1)}%-30s ${row.getInt(2)}%3d ${row.getInt(3)}%3d")
    }
  }
}
