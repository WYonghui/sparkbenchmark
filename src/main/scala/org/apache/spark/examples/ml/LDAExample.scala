package org.apache.spark.examples.ml

// scalastyle:off println
// $example on$
import org.apache.spark.ml.clustering.LDA
// $example off$
import org.apache.spark.sql.SparkSession


/**
  * An example demonstrating LDA.
  * Run with
  * {{{
  * bin/run-example ml.LDAExample
  * }}}
  * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.ml.LDAExample \
      spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar ml.LDAExample \
      /hadoop-5nodes/dataset/lda/sample_lda_libsvm_data.txt
  */
object LDAExample {

  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      System.out.print("Usage: LDAExample <appName> <file1>")
      System.exit(-1)
    }

    // Creates a SparkSession
    val spark = SparkSession
      .builder
//      .appName(s"${this.getClass.getSimpleName}")
        .appName(args(0))
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
//      .load("data/mllib/sample_lda_libsvm_data.txt")
      .load(args(1))

    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
    // $example off$

    spark.stop()
  }
}
