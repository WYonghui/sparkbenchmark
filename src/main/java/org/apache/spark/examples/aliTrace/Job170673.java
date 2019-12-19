package org.apache.spark.examples.aliTrace;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * spark-submit --master spark://node91:7077 --deploy-mode client --class org.apache.spark.examples.aliTrace.Job170673 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job170673 15 ratings.csv tags.csv 0
 */
public class Job170673 {
    private static Logger LOG = LoggerFactory.getLogger(Job170673.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            LOG.error("Usage: Job170673 <appName> <parallelism> <inputFile1> <inputFile2>");
            System.exit(-1);
        }

        SparkSession.Builder sparkBuilder = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", Integer.parseInt(args[1]));

        if (args.length == 5) {
            sparkBuilder.config("spark.locality.wait", Integer.parseInt(args[4]));
        }

        SparkSession spark = sparkBuilder.getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String ratingsFile = args[2];
        String tagsFile = args[3];

        JavaPairRDD<Integer, Integer> ratings = sc.textFile(ratingsFile)
                .mapPartitionsToPair(new StringMapPartition(9000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> reducedRatings1 = ratings
                .mapPartitionsToPair(new IntegerMapPartition(8000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(3000));

        JavaPairRDD<Integer, Integer> reducedRatings2 = ratings
                .mapPartitionsToPair(new IntegerMapPartition(4000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(13000));

        JavaPairRDD<Integer, Integer> reducedRatings3 = ratings
                .mapPartitionsToPair(new IntegerMapPartition(6000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(7000));

        JavaPairRDD<Integer, Integer> tags = sc.textFile(tagsFile)
                .mapPartitionsToPair(new StringMapPartition(4000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(6000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(4000));

        JavaPairRDD<Integer, Integer> result = reducedRatings1.join(reducedRatings2)
                .union((JavaPairRDD<Integer, Tuple2<Integer, Integer>>)tags.join(reducedRatings3))
                .mapPartitionsToPair(new Tuple2MapPartition(3000));

        result.collect();

    }


    private static class StringMapPartition implements PairFlatMapFunction<Iterator<String>, Integer, Integer> {

        private Integer sleepTime;

        StringMapPartition(Integer sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
            Thread.sleep(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class IntegerMapPartition implements PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer> {
        private Integer sleepTime;

        IntegerMapPartition(Integer sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
            Thread.sleep(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class Tuple2MapPartition implements PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Integer, Integer> {
        private Integer sleepTime;

        Tuple2MapPartition(Integer sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> tuple2Iterator) throws Exception {
            Thread.sleep(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class ReducedFunction implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer integer, Integer integer2) throws Exception {

            return integer;
        }
    }


}
