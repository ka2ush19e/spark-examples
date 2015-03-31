package core

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object Pipe {

  def main(args: Array[String]) {
    run(args)
  }

  def run(args: Array[String]) {
    val conf = new SparkConf().setAppName("Pipe")
    val sc = new SparkContext(conf)

    val columnCountScript = "./src/main/scripts/columncount.py"
    val columnCountScriptName = "columncount.py"
    sc.addFile(columnCountScript)

    val lines = sc.parallelize(Array("1,2,3", "4,5", "6", "7,8,9,10"))

    lines.pipe(SparkFiles.get(columnCountScriptName)).foreach(println)

    sc.stop()
  }
}
