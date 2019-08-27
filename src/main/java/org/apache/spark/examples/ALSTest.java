package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * hdfs dfs -rm -r -f /wyh/output/ALS/*
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.ALSTest \
 spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
 hdfs://node91:9000/wyh/dataset/ALS/ml-100k/alsTest.data 10 2 hdfs://node91:9000/wyh/output/ALS
 */
public class ALSTest {

    private static final Logger log = LoggerFactory.getLogger(ALSTest.class);


    private static class ParseRating implements Function<String, Rating> {
        final Pattern COMMA = Pattern.compile(",");
        @Override
        public Rating call(String s) throws Exception {
            String[] strs = COMMA.split(s);
            return new Rating(Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Double.parseDouble(strs[2]));
        }

    }

    private static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
        @Override
        public String call(Tuple2<Object, double[]> objectTuple2) throws Exception {
            return objectTuple2._1() + "," + Arrays.toString(objectTuple2._2());
        }
    }

    public static void main(String[] args) throws IOException {

        SparkSession ss = SparkSession
                .builder()
                .appName("ALSTest")
                .getOrCreate();

        JavaRDD<String> ratingFile = ss.read().textFile(args[0]).javaRDD();

        JavaRDD<Rating> ratingRDD = ratingFile.map(new ParseRating());

        Integer rank = Integer.parseInt(args[1]);
        Integer iterations = Integer.parseInt(args[2]);
        String outputDir = args[3];

        log.info("-----------------------开始训练数据-----------------------");
        MatrixFactorizationModel model = ALS.train(ratingRDD.rdd(), rank, iterations, 0.01);
        log.info("-----------------------训练完成---------------------------");

        model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir+"/userFeatures");
//        model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir+"/productFeatures");
        log.info("Final user/product features written to " + outputDir);
        System.out.println("Final user/product features written to " + outputDir);
    }
}
