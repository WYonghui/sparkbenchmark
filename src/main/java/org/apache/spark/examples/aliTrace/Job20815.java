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
 * spark-submit --master spark://node91:7077 --deploy-mode client --class org.apache.spark.examples.aliTrace.Job20815 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job20815 15 ratings.csv tags.csv tags2.csv 0
 */
public class Job20815 {
    private static Logger LOG = LoggerFactory.getLogger(Job20815.class);

    public static void main(String[] args) {
        if (args.length < 5) {
            LOG.error("Usage: Job20815 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3>");
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
                .mapPartitionsToPair(new StringMapPartition(1000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> mappedRatings = ratings
                .mapPartitionsToPair(new IntegerMapPartition(200))
                .reduceByKey(new ReducedFunction());

        JavaPairRDD<Integer, Integer> reducedRatings = ratings
                .mapPartitionsToPair(new IntegerMapPartition(200))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(15000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new StringMapPartition(10000));

        JavaPairRDD<Integer, Integer> join1 = reducedRatings.join(tags1)
                .mapPartitionsToPair(new Tuple2MapPartition(9000));

        JavaPairRDD<Integer, Integer> join2 = join1.join(reducedRatings)
                .mapPartitionsToPair(new Tuple2MapPartition(1000));

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new StringMapPartition(6000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> join3 = join2.join(tags2)
                .mapPartitionsToPair(new Tuple2MapPartition(2000));

        JavaPairRDD<Integer, Integer> result = join3.join(tags2)
                .join(mappedRatings)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(5000);
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
