package core

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object Join {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(conf)

    val persons = sc.parallelize(Array(
      ("Adam", "San francisco"),
      ("Bob", "San francisco"),
      ("Taro", "Tokyo"),
      ("Charles", "New York")
    ))

    val cities = sc.parallelize(Array(
      ("Tokyo", "Japan"),
      ("San francisco", "America"),
      ("Beijing", "China")
    ))

    println("### Join ###")
    persons.map { case (name, city) => (city, name) }.join(cities).collect().foreach {
      case (city, (name, country)) =>
        println(f"$name%-10s $country%-10s $city%-10s")
    }
    println()

    println("### Left outer join ###")
    persons.map { case (name, city) => (city, name) }.leftOuterJoin(cities).collect().foreach {
      case (city, (name, countryOpt)) =>
        println(f"""$name%-10s ${countryOpt.getOrElse("")}%-10s $city%-10s""")
    }
    println()

    println("### Right outer join ###")
    persons.map { case (name, city) => (city, name) }.rightOuterJoin(cities).collect().foreach {
      case (city, (nameOpt, country)) =>
        println(f"""${nameOpt.getOrElse("")}%-10s $country%-10s $city%-10s""")
    }
    println()

    println("### Full outer join ###")
    persons.map { case (name, city) => (city, name) }.fullOuterJoin(cities).collect().foreach {
      case (city, (nameOpt, countryOpt)) =>
        println(f"""${nameOpt.getOrElse("")}%-10s ${countryOpt.getOrElse("")}%-10s $city%-10s""")
    }
    println()

    println("### Grouping ###")
    persons.map { case (name, city) => (city, name) }.cogroup(cities).collect().foreach {
      case (city, (names, countries)) =>
        println(f"""$city%-20s ${names.mkString(",")}%-20s ${countries.mkString(",")}%-20s""")
    }
    println()

    sc.stop()
  }
}
