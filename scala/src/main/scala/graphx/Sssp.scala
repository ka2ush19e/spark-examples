package graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sssp {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Single Source Shortest Path ")
    val sc = new SparkContext(conf)

    val vertices = sc.parallelize(Array(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E")))

    val edges = sc.parallelize(Array(
      Edge(1L, 2L, 10), Edge(1L, 3L, 5),
      Edge(2L, 3L, 2), Edge(2L, 4L, 1),
      Edge(3L, 2L, 3), Edge(3L, 4L, 9), Edge(3L, 5L, 2),
      Edge(4L, 5L, 4),
      Edge(5L, 4L, 6), Edge(5L, 1L, 7)))

    val graph = Graph(vertices, edges)
    val initialGraph = graph.mapVertices { (vid, _) =>
      if (vid == 1L) 0.0 else Double.PositiveInfinity
    }.persist()

    (0 to 5).foreach { i =>
      computeSsp(initialGraph, vertices, i)
    }
  }

  private[this] def computeSsp(
    initialGraph: Graph[Double, Int],
    vertices: RDD[(Long, String)],
    maxIterations: Int) {
    val sssp = initialGraph.pregel(Double.PositiveInfinity, maxIterations)(
      vprog = { (id, dist, newDist) =>
        math.min(dist, newDist)
      },
      sendMsg = { t =>
        if (t.srcAttr + t.attr < t.dstAttr) {
          Iterator((t.dstId, t.srcAttr + t.attr))
        } else {
          Iterator.empty
        }
      },
      mergeMsg = { (x, y) =>
        math.min(x, y)
      }
    )

    println(s"### Iteration $maxIterations ###")
    sssp.outerJoinVertices(vertices) {
      case (vid, dist, Some(name)) => (dist, name)
      case (vid, dist, None) => (dist, "Unknown")
    }.vertices.collect().sortBy(_._1).foreach { case (vid, (dist, name)) =>
      println(f"$name: ${dist.toInt}")
    }
    println()
  }
}
