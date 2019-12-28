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
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job1077388 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1077388 15 ratings.csv tags.csv 0
 */
public class Job1077388 {

    private static Logger LOG = LoggerFactory.getLogger(Job1077388.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            LOG.error("Usage: Job1077388 <appName> <parallelism> <inputFile1> <inputFile2>");
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
                .mapPartitionsToPair(new StringMapPartition(8000L))
                .reduceByKey(new ReducedFunction()); // single stage

        JavaPairRDD<Integer, Integer> tags = sc.textFile(tagsFile)
                .mapPartitionsToPair(new StringMapPartition(14000L))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> movieAndUserId = tags
                .mapPartitionsToPair(new IntegerMapPartition(5000L));

        JavaPairRDD<Integer, Integer> movieAndTag = tags
                .mapPartitionsToPair(new IntegerMapPartition(6000L))
                .reduceByKey(new ReducedFunction());

        JavaPairRDD<Integer, Integer> result = ratings.join(movieAndUserId)
                .join(movieAndTag)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        (new Job1077388()).sleepFunction(500L);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        result.collect();
    }

    private void sleepFunction(Long time) {
        Long start = System.currentTimeMillis();
        while (true) {
            double a = 12332.234;
            double b = 23545342.2342;
            double c = a * b;
            if ((System.currentTimeMillis() - start) > time) {
                break;
            }
        }
    }

    private static class StringMapPartition implements PairFlatMapFunction<Iterator<String>, Integer, Integer> {

        private Long sleepTime;

        StringMapPartition(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
            (new Job1077388()).sleepFunction(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class IntegerMapPartition implements PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer> {
        private Long sleepTime;

        IntegerMapPartition(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
            (new Job1077388()).sleepFunction(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class Tuple2MapPartition implements PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Integer, Integer> {
        private Long sleepTime;

        Tuple2MapPartition(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> tuple2Iterator) throws Exception {
            (new Job1077388()).sleepFunction(sleepTime);
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
