package org.apache.spark.examples.ml;

import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example demonstrating LDA.
 * Run with
 * <pre>
 * bin/run-example ml.JavaLDAExample
 * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.ml.JavaLDAExample \
        spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar JavaLDAExample \
        /hadoop-5nodes/dataset/lda/sample_lda_libsvm_data.txt
 * </pre>
 */
public class JavaLDAExample {
    private static Logger LOG = LoggerFactory.getLogger(JavaLDAExample.class);

    public static void main(String[] args) {

        if (args.length < 2) {
            LOG.info("Usage : JavaLDAExample <appName> <inputFile>");
            System.exit(-1);
        }

        // Creates a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName(args[0])
                .getOrCreate();

        // $example on$
        // Loads data.
        String path = args[1];
        Dataset<Row> dataset = spark.read().format("libsvm")
                .load(path);

        // Trains a LDA model.
        LDA lda = new LDA().setK(10).setMaxIter(10);
        LDAModel model = lda.fit(dataset);

//        double ll = model.logLikelihood(dataset);
//        double lp = model.logPerplexity(dataset);
//        System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
//        System.out.println("The upper bound bound on perplexity: " + lp);

        // Describe topics.
//        Dataset<Row> topics = model.describeTopics(3);
//        System.out.println("The topics described by their top-weighted terms:");
//        topics.show(false);

        // Shows the result.
        Dataset<Row> transformed = model.transform(dataset);
        transformed.show(false);
        // $example off$

        spark.stop();
    }

}
