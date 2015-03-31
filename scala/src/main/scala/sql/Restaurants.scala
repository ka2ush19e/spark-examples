package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Restaurants {

  case class Restaurant(
    id: String,
    name: String,
    property: String,
    alphabet: String,
    name_kana: String,
    pref_id: String,
    area_id: String)

  case class Pref(id: String, name: String)

  case class Area(id: String, pref_id: String, name: String)

  def main(args: Array[String]) {
    run(args(0), args(1), args(2))
  }

  def run(restaurantFile: String, prefFile: String, areaFile: String) {
    val conf = new SparkConf().setAppName("Restaurants")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    import hiveCtx._

    val restaurants = sc.textFile(restaurantFile)
      .filter(!_.startsWith("id"))
      .map(_.split(","))
      .filter(_.size >= 7)
      .map(p => Restaurant(p(0), p(1), p(2), p(3), p(4), p(5), p(6)))
    restaurants.registerTempTable("restaurants")

    val prefs = sc.textFile(prefFile)
      .filter(!_.startsWith("id"))
      .map(_.split(","))
      .filter(_.size >= 2)
      .map(p => Pref(p(0), p(1)))
    prefs.registerTempTable("prefs")

    val areas = sc.textFile(areaFile)
      .filter(!_.startsWith("id"))
      .map(_.split(","))
      .filter(_.size >= 3)
      .map(p => Area(p(0), p(1), p(2)))
    areas.registerTempTable("areas")

    println("### Restaurants in Tokyo ###")
    val restaurantsInTokyo = hiveCtx.sql(
      """SELECT
        |    r.id,
        |    r.alphabet,
        |    p.name
        |FROM
        |    restaurants r
        |INNER JOIN prefs p
        |    ON p.id = r.pref_id
        |WHERE
        |    p.id = '13'
        |AND r.alphabet <> ''
        |LIMIT
        |    10
      """.stripMargin)

    restaurantsInTokyo.collect().foreach { row =>
      println(f"${row.getString(0)}%3s ${row.getString(1)}%-30s ${row.getString(2)}%-15s")
    }
  }
}
