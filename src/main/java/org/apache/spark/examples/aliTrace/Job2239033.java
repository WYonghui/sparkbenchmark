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
 * spark-submit --master spark://node91:7077 --deploy-mode client --class org.apache.spark.examples.aliTrace.Job2239033 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job2239033 15 ratings.csv tags.csv tags2.csv 0
 */
public class Job2239033 {
    private static Logger LOG = LoggerFactory.getLogger(Job2239033.class);

    public static void main(String[] args) {
        if (args.length < 5) {
            LOG.error("Usage: Job2239033 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3>");
            System.exit(-1);
        }

        SparkSession.Builder sparkBuilder = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", Integer.parseInt(args[1]));

        if (args.length == 6) {
            sparkBuilder.config("spark.locality.wait", Integer.parseInt(args[5]));
        }

        SparkSession spark = sparkBuilder.getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String ratingsFile = args[2];
        String tags1File = args[3];
        String tags2File = args[4];

        JavaPairRDD<Integer, Integer> ratings = sc.textFile(ratingsFile)
                .mapPartitionsToPair(new StringMapPartition(7000L))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> mappedRatings = ratings
                .mapPartitionsToPair(new IntegerMapPartition(7000L))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(6000L));

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new StringMapPartition(4000L))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(7000L));

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new StringMapPartition(3000L));

        JavaPairRDD<Integer, Integer> join1 = tags1.join(tags2)
                .join(ratings)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(3000);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        JavaPairRDD<Integer, Integer> result = join1.join(mappedRatings)
                .mapPartitionsToPair(new Tuple2MapPartition(200L));

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
            (new Job2239033()).sleepFunction(sleepTime);
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
            (new Job2239033()).sleepFunction(sleepTime);
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
            (new Job2239033()).sleepFunction(sleepTime);
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
