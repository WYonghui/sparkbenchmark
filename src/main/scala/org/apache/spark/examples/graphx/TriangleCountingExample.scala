package org.apache.spark.examples.graphx

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession


/**
  * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.graphx.TriangleCountingExample \
      spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar TriangleCountingExample \
      /hadoop-5nodes/dataset/graphx/follows.txt /hadoop-5nodes/dataset/graphx/users.txt
  */
object TriangleCountingExample {

  def main(args: Array[String]): Unit = {

    if (args.size < 3) {
      System.out.print("Usage: TriangleCountingExample <appName> <file1> <file2>")
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
    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, args(1), true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile(args(2)).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}
