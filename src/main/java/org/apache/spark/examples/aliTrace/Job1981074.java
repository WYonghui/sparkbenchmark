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
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job1981074 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1981074 15 ratings.csv tags1.csv tags2.csv tags3.csv 0
 */
public class Job1981074 {

    private static Logger LOG = LoggerFactory.getLogger(Job1981074.class);

    public static void main(String[] args) {
        if (args.length < 6) {
            LOG.error("Usage: Job1981074 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3> <inputFile4>");
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
                .mapPartitionsToPair(new StringMapPartition(10000))
                .reduceByKey(new ReducedFunction())
                .cache();

        JavaPairRDD<Integer, Integer> ratingsPartition1 = ratings
                .mapPartitionsToPair(new IntegerMapPartition(5000));

        JavaPairRDD<Integer, Integer> ratingsPartition2 = ratings
                .mapPartitionsToPair(new IntegerMapPartition(6000));

        JavaPairRDD<Integer, Integer> tags3 = sc.textFile(tags3File)
                .mapPartitionsToPair(new StringMapPartition(5000))
                .reduceByKey(new ReducedFunction())
                .mapPartitionsToPair(new IntegerMapPartition(4000));

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new StringMapPartition(3000));

        JavaPairRDD<Integer, Integer> join1 = tags3.join(tags2)
                .mapPartitionsToPair(new Tuple2MapPartition(6000));

        JavaPairRDD<Integer, Integer> join2 = join1.join(ratingsPartition1)
                .mapPartitionsToPair(new Tuple2MapPartition(5000));

        JavaPairRDD<Integer, Integer> join3 = join2.join(ratingsPartition2)
                .mapPartitionsToPair(new Tuple2MapPartition(5000));

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new StringMapPartition(3000));

        JavaPairRDD<Integer, Integer> result = tags1.join(join3)
                .mapPartitionsToPair(new Tuple2MapPartition(1000));

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
