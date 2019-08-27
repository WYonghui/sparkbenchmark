package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Example using MLlib ALS from Java.
 * hdfs dfs -rm -r -f hdfs://node91:9000/wyh/output/ALS/*
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.JavaALS \
    --name JavaALS spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
    hdfs://node91:9000/wyh/dataset/ALS/test.data 10 2 hdfs://node91:9000/wyh/output/ALS
 */
public final class JavaALS {

    static class ParseRating implements Function<String, Rating> {
        private static final Pattern COMMA = Pattern.compile(",");

        @Override
        public Rating call(String line) {
            String[] tok = COMMA.split(line);
            int x = Integer.parseInt(tok[0]);
            int y = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            return new Rating(x, y, rating);
        }
    }

    static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
        @Override
        public String call(Tuple2<Object, double[]> element) {
            return element._1() + "," + Arrays.toString(element._2());
        }
    }

    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println(
                    "Usage: JavaALS <ratings_file> <rank> <iterations> <output_dir> [<blocks>]");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaALS");
        int rank = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        String outputDir = args[3];
        int blocks = -1;
        if (args.length == 5) {
            blocks = Integer.parseInt(args[4]);
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<Rating> ratings = lines.map(new ParseRating());

        MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);
        System.out.println("calling ALS.train hahahha");

        model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
                outputDir + "/userFeatures");
        model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
                outputDir + "/productFeatures");
        System.out.println("Final user/product features written to " + outputDir);

        sc.stop();
    }


}
