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
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job898272 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job898272 15 ratings.csv tags.csv tags2.csv tags3.csv 0
 */
public class Job898272 {
    private static Logger LOG = LoggerFactory.getLogger(Job898272.class);

    public static void main(String[] args) {
        if (args.length < 6) {
            LOG.error("Usage: Job898272 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3> <inputFile4>");
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
                .mapPartitionsToPair(new StringMapPartition(24000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new StringMapPartition(12000))
                .reduceByKey(new ReducedFunction());

        JavaPairRDD<Integer, Integer> join1 = tags1.join(ratings)
                .mapPartitionsToPair(new Tuple2MapPartition(15000));

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new StringMapPartition(12000))
                .reduceByKey(new ReducedFunction());

        JavaPairRDD<Integer, Integer> join2 = tags2.join(ratings)
                .mapPartitionsToPair(new Tuple2MapPartition(12000));

        JavaPairRDD<Integer, Integer> tags3 = sc.textFile(tags3File)
                .mapPartitionsToPair(new StringMapPartition(15000))
                .reduceByKey(new ReducedFunction());

        JavaPairRDD<Integer, Integer> result = join1.join(join2)
                .join(tags3)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(300);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

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
