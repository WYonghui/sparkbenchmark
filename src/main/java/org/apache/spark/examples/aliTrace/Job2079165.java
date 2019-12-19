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
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job2079165 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job2079165 15 ratings.csv tags1.csv tags2.csv tags3.csv 0
 */
public class Job2079165 {
    private static Logger LOG = LoggerFactory.getLogger(Job2079165.class);

    public static void main(String[] args) {
        if (args.length < 5) {
            LOG.error("Usage: Job2079165 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3>");
            System.exit(-1);
        }

        SparkSession.Builder sparkBuilder = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", Integer.parseInt(args[1]));

        if (args.length == 7) {
            sparkBuilder.config("spark.locality.wait", Integer.parseInt(args[6]));
        }

        SparkSession spark = sparkBuilder.getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String ratingsFile = args[2];
        String tags1File = args[3];
        String tags2File = args[4];
        String tags3File = args[5];

        JavaPairRDD<Integer, Integer> ratings = sc.textFile(ratingsFile)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(10000);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });
        JavaPairRDD<Integer, Integer> ratingsReducedByKey = ratings
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                        Thread.sleep(12000);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer2;
                    }
                }).cache();

        JavaPairRDD<Integer, Integer> ratingsMapPartition1 = ratingsReducedByKey
                .mapPartitionsToPair(new Tuple2PairFlatMap(5000L));

        JavaPairRDD<Integer, Integer> ratingsMapPartition2 = ratingsReducedByKey
                .mapPartitionsToPair(new Tuple2PairFlatMap(6000L));

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new StringPairFlatMap(6000L))
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                });

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new StringPairFlatMap(4000L))
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                });

        JavaPairRDD<Integer, Integer> tags3 = sc.textFile(tags3File)
                .mapPartitionsToPair(new StringPairFlatMap(5000L))
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                });
        JavaPairRDD<Integer, Integer> join1 = tags1.join(tags2).join(tags3)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(8000);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        JavaPairRDD<Integer, Integer> result = join1.join(ratingsMapPartition1).join(ratingsMapPartition2)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(2000L);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        result.collect();

    }

    private static class Tuple2PairFlatMap implements PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer> {

        private Long sleepTime;

        Tuple2PairFlatMap(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
            Thread.sleep(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

    private static class StringPairFlatMap implements PairFlatMapFunction<Iterator<String>, Integer, Integer> {

        private Long sleepTime;

        StringPairFlatMap(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
            Thread.sleep(sleepTime);
            return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
        }
    }

}
