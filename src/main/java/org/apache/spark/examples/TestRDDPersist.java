package org.apache.spark.examples;

import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.TestRDDPersist \
     --name TestRDDPersist spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
     hdfs://node39:9000/hadoop-5nodes/dataset/als/alsTest.data
 */
public class TestRDDPersist {
    private static Logger LOG = LoggerFactory.getLogger(TestRDDPersist.class);

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: TestRDDPersist <ratings_file>");
            System.exit(-1);
        }

        SparkSession ss = SparkSession
                .builder()
                .appName("TestRDDPersist")
                .config("spark.default.parallelism", 22)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        JavaRDD<Tuple3<Integer, Integer, Double>> ratings = sc.textFile(args[0]).map(
                new Function<String, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple3<Integer, Integer, Double> call(String s) throws Exception {
                        String[] items = s.split(",");

//                        userId, movieId, rating
                        return new Tuple3<Integer, Integer, Double>(
                                Integer.parseInt(items[0]),
                                Integer.parseInt(items[1]),
                                Double.parseDouble(items[2]));
                    }
                }
        );

        JavaPairRDD<Integer, Tuple2<Integer, Double>> reducedRatings = ratings.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple3<Integer, Integer, Double>>, Integer, Tuple2<Integer, Double>>() {
            @Override
            public Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> call(Iterator<Tuple3<Integer, Integer, Double>> tuple3Iterator) throws Exception {
                List<Tuple2<Integer, Tuple2<Integer, Double>>> list = new ArrayList<>();
                while (tuple3Iterator.hasNext()) {
                    Tuple3<Integer, Integer, Double> tuple3 = tuple3Iterator.next();
                    list.add(new Tuple2<>(tuple3._1(), new Tuple2<>(tuple3._2(), tuple3._3())));
                }

                return list.iterator();
            }
        }).reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Double> integerDoubleTuple2, Tuple2<Integer, Double> integerDoubleTuple22) throws Exception {
                Tuple2<Integer, Double> tuple2 = new Tuple2<Integer, Double>(integerDoubleTuple2._1, (integerDoubleTuple2._2 + integerDoubleTuple22._2) / 2);
                return tuple2;
            }
        }).cache();

        JavaPairRDD<Integer, Double> usersAndRating = reducedRatings.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Double>>>, Integer, Double>() {
            @Override
            public Iterator<Tuple2<Integer, Double>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> tuple2Iterator) throws Exception {
                List<Tuple2<Integer, Double>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Thread.sleep(150);
                    Tuple2<Integer, Tuple2<Integer, Double>> tuple2Tuple2 = tuple2Iterator.next();
                    list.add(new Tuple2<>(tuple2Tuple2._1, tuple2Tuple2._2._2));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Integer, String> productAndRating = reducedRatings.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, Double>>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, Tuple2<Integer, Double>> integerTuple2Tuple2) throws Exception {
                Thread.sleep(100);
                return new Tuple2<>(integerTuple2Tuple2._1, "");
            }
        }) ;


        JavaPairRDD<Integer, Tuple2<Double, String>> join = (JavaPairRDD<Integer, Tuple2<Double, String>>) usersAndRating.join(productAndRating);

        LOG.info("result is " + join.first()._1);

//        ratings.unpersist();

    }

}
