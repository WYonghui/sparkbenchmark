package org.apache.spark.examples.mllib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * Compute the similar columns of a matrix, using cosine similarity.
  *
  * The input matrix must be stored in row-oriented dense format, one line per row with its entries
  * separated by space. For example,
  * {{{
  * 0.5 1.0
  * 2.0 3.0
  * 4.0 5.0
  * }}}
  * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
  *
  * Example invocation:
  *
  * bin/run-example mllib.CosineSimilarity \
  * --threshold 0.1 data/mllib/sample_svm_data.txt
  * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.mllib.CosineSimilarity \
  *   spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
  *   --threshold 0.015 /rdma-spark-0.9.5/hibench/HiBench/SVD/Output/
  */
object CosineSimilarity {
  case class Params(inputFile: String = null, threshold: Double = 0.1)
    extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("CosineSimilarity") {
      head("CosineSimilarity: an example app.")
      opt[Double]("threshold")
        .required()
        .text(s"threshold similarity: to tradeoff computation vs quality estimate")
        .action((x, c) => c.copy(threshold = x))
      arg[String]("<inputFile>")
        .required()
        .text(s"input file, one row per line, space-separated")
        .action((x, c) => c.copy(inputFile = x))
      note(
        """
          |For example, the following command runs this app on a dataset:
          |
          | ./bin/spark-submit  --class org.apache.spark.examples.mllib.CosineSimilarity \
          | examplesjar.jar \
          | --threshold 0.1 data/mllib/sample_svm_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("CosineSimilarity")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val rows = sc.textFile(params.inputFile).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }.cache()
//    val rows : RDD[Vector] = sc.objectFile(params.inputFile).cache()
    val mat = new RowMatrix(rows)

    // Compute similar columns perfectly, with brute force.
    val exact = mat.columnSimilarities()

    // Compute similar columns with estimation using DIMSUM
    val approx = mat.columnSimilarities(params.threshold)

    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()

    println(s"Average absolute error in estimate is: $MAE")

    sc.stop()
  }
}
