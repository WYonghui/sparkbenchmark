package org.apache.spark.examples.graphx

// $example on$
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * A connected components algorithm example.
  * The connected components algorithm labels each connected component of the graph
  * with the ID of its lowest-numbered vertex.
  * For example, in a social network, connected components can approximate clusters.
  * GraphX contains an implementation of the algorithm in the
  * [`ConnectedComponents` object][ConnectedComponents],
  * and we compute the connected components of the example social network dataset.
  *
  * Run with
  * {{{
  * bin/run-example graphx.ConnectedComponentsExample
  * }}}
  * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.graphx.ConnectedComponentsExample \
        spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar ConnectedComponentsExample \
        /hadoop-5nodes/dataset/graphx/followers.txt /hadoop-5nodes/dataset/graphx/users.txt
  */
object ConnectedComponentsExample {
  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      System.out.print("Usage: ConnectedComponentsExample <appName> <file1> <file2>")
      System.exit(-1)
    }

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
//      .appName(s"${this.getClass.getSimpleName}")
        .appName(args(0))
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the graph as in the PageRank example
//    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    val graph = GraphLoader.edgeListFile(sc, args(1))

    // Find the connected components
//    val cc = graph.connectedComponents().vertices
    graph.connectedComponents()
    // Join the connected components with the usernames
//    val users = sc.textFile("data/graphx/users.txt").map { line =>
//    val users = sc.textFile(args(2)).map { line =>
//
//      val fields = line.split(",")
//      (fields(0).toLong, fields(1))
//    }
//    val ccByUsername = users.join(cc).map {
//      case (id, (username, cc)) => (username, cc)
//    }
    // Print the result
//    println(ccByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}
// scalastyle:on println