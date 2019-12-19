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
import java.util.List;

/**
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job1981328 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1981328 15 ratings.csv tags1.csv tags2.csv 0
 */
public class Job1981328 {
    private static Logger LOG = LoggerFactory.getLogger(Job1981328.class);

    public static void main(String[] args) {
        if (args.length < 5) {
            LOG.error("Usage: Job1981328 <appName> <parallelism> <inputFile1> <inputFile2> <inputFile3>");
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

        JavaPairRDD<Integer, Integer> tags1 = sc.textFile(tags1File)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(2100);
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        return list.iterator();
                    }
                });
        JavaPairRDD<Integer, Integer> tags1ReducedByKey = tags1.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                Thread.sleep(3400);
                return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
            }
        });

        JavaPairRDD<Integer, Integer> tags2 = sc.textFile(tags2File)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(4900);
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        return list.iterator();
                    }
                });
        JavaPairRDD<Integer, Integer> tags2ReducedByKey = tags2.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                Thread.sleep(3200);
                return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
            }
        });

        JavaPairRDD<Integer, Integer> join1 = ((JavaPairRDD<Integer, Tuple2<Integer, Integer>>)tags1ReducedByKey
                .join(tags2ReducedByKey))
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(3800);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        JavaPairRDD<Integer, Integer> ratings = sc.textFile(ratingsFile)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(8200);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                }).cache();

        JavaPairRDD<Integer, Integer> rating1MapPartitions = ratings
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                        Thread.sleep(2700);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                });

        JavaPairRDD<Integer, Integer> ratings2MapPartitions = ratings
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                        Thread.sleep(2200);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                });

        JavaPairRDD<Integer, Integer> result = ((JavaPairRDD<Integer, Tuple2<Integer, Integer>>)join1
                .join(ratings2MapPartitions))
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> tuple2Iterator) throws Exception {
                        Thread.sleep(800);
                        return (new ArrayList<Tuple2<Integer, Integer>>()).iterator();
                    }
                }).union(rating1MapPartitions);

        result.collect();

    }

}
