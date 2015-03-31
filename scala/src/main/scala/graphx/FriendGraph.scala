package graphx

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object FriendGraph {

  def main(args: Array[String]) {
    run()
  }

  def run() {
    val conf = new SparkConf().setAppName("Friends Graph")
    val sc = new SparkContext(conf)

    val graph = FriendGraphBuilder.build(sc).persist()

    println("### Show Vertices ###")
    graph.vertices.foreach { case (vid, (name, age)) =>
      println(s"$name is $age years old")
    }
    println()

    println("### Show Edges ###")
    graph.edges.foreach { e =>
      println(s"${e.srcId} likes ${e.dstId}, ${e.attr}th in the world")
    }
    println()

    println("### Show Triplets ###")
    graph.triplets.foreach { t =>
      println(s"${t.srcAttr._1} likes ${t.dstAttr._1}, ${t.attr}th in the world")
    }
    println()

    println("### Edit Vertices ###")
    graph.mapVertices { case (vid, (name, age)) =>
      (name, age + 10)
    }.vertices.foreach { case (vid, (name, age)) =>
      println(s"10 years later, $name is $age years old")
    }
    println()

    println("### Edit Edges ###")
    graph.mapEdges(_.attr + 1).edges.foreach { e =>
      println(s"${e.srcId} likes ${e.dstId}, ${e.attr}th in the world")
    }
    println()

    println("### Edit Triplets ###")
    graph.mapTriplets { t =>
      t.srcAttr._2 + t.dstAttr._2
    }.triplets.foreach { t =>
      println(s"${t.srcAttr._1}'s age + ${t.dstAttr._1}'s age = ${t.attr}")
    }
    println()

    println("### Subgraph ###")
    graph.subgraph(
      epred = e => e.attr > 5,
      vpred = {
        case (vid, (name, age)) => age > 30
      }
    ).triplets.foreach { t =>
      println(s"${t.srcAttr._1} likes ${t.dstAttr._1}")
    }
    println()

    println("### Degrees ###")
    graph.outerJoinVertices(graph.inDegrees) {
      case (vid, (name, age), Some(d)) => (name, d)
      case (vid, (name, age), None) => (name, 0)
    }.vertices.foreach { case (vid, (name, d)) =>
      println(s"$name's inDegree is $d")
    }
    println()

    println("### Neighbors ###")
    graph.collectNeighborIds(EdgeDirection.Out).foreach { case (vid, neighborIds) =>
      val n = neighborIds.mkString(", ")
      println(s"$vid's neighbors: $n")
    }
    println()

    graph.collectNeighbors(EdgeDirection.In).foreach { case (vid, neighbors) =>
      val n = neighbors.map { case (_, (name, _)) => name }.mkString(", ")
      println(s"$vid's neighbors: $n")
    }
    println()

    println("### Aggregate ###")
    graph.aggregateMessages[(Long, Long)](
      sendMsg = { t =>
        t.sendToDst(t.srcAttr._2, 1L)
      },
      mergeMsg = { (x, y) =>
        (x._1 + y._1, x._2 + y._2)
      }
    ).mapValues { (id, value) =>
      value match {
        case (sum, count) =>
          sum * 1.0 / count
      }
    }.foreach { case (vid, avgAge) =>
      println(vid, avgAge)
    }
    println()

    println("### Friends of friends of Charlie ###")
    val initialGraph = graph.mapVertices((vid, _) => if (vid == 3L) 2 else -1)
    val friendsOfFriends = initialGraph.pregel(-1, 2)(
      vprog = { (id, dist, newDist) =>
        math.max(dist, newDist)
      },
      sendMsg = { t =>
        if (t.srcAttr < 0 && t.dstAttr < 0) {
          Iterator.empty
        } else if (t.dstAttr < 0) {
          Iterator((t.dstId, t.srcAttr - 1))
        } else {
          Iterator.empty
        }
      },
      mergeMsg = { (a, b) =>
        math.max(a, b)
      }
    )
    friendsOfFriends.vertices.collect().filter(_._2 >= 0).foreach(println)
  }
}
