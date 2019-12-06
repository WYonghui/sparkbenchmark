package org.apache.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.Job1077229 \
      spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1077229 15 ratings.csv tags.csv 0
 */
public class Job1077229 {
    private static Logger LOG = LoggerFactory.getLogger(Job1077229.class);

    public static void main(String[] args) {

        if (args.length < 4) {
            LOG.error("Usage: Job1077229 <appName> <parallelism> <inputFile1> <inputFile2>");
            System.exit(-1);
        }

        SparkSession.Builder sparkBuilder = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", Integer.parseInt(args[1]));

        if (args.length == 5) {
            sparkBuilder.config("", Integer.parseInt(args[4]));
        }
        SparkSession spark = sparkBuilder.getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String ratingsFile = args[2];
        String moviesFile = args[3];

        JavaRDD<Tuple3<Integer, Integer, Double>> ratings = sc.textFile(ratingsFile).map(
                new Function<String, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple3<Integer, Integer, Double> call(String s) throws Exception {
                        String[] items = s.split(",");

//                        movieId, userId, rating
                        return new Tuple3<Integer, Integer, Double>(
                                Integer.parseInt(items[1]),
                                Integer.parseInt(items[0]),
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
                        Tuple3<Integer, Integer, Double> tuple = tuple3Iterator.next();
                        list.add(new Tuple2<>(tuple._1(), new Tuple2<>(tuple._2(), tuple._3())));
                    }

                    return list.iterator();
                }
            })
            .reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                @Override
                public Tuple2<Integer, Double> call(Tuple2<Integer, Double> integerDoubleTuple2, Tuple2<Integer, Double> integerDoubleTuple22) throws Exception {
                    Tuple2<Integer, Double> tuple = new Tuple2<Integer, Double>(integerDoubleTuple2._1, (integerDoubleTuple2._2 + integerDoubleTuple22._2) / 2);
                    return tuple;
                }
            }).cache();

        JavaPairRDD<Integer, Double> moviesAndRating = reducedRatings.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Double>>>, Integer, Double>() {
            @Override
            public Iterator<Tuple2<Integer, Double>> call(Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> tuple2Iterator) throws Exception {
                List<Tuple2<Integer, Double>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Thread.sleep(3);
                    Tuple2<Integer, Tuple2<Integer, Double>> tuple2Tuple2 = tuple2Iterator.next();
                    list.add(new Tuple2<>(tuple2Tuple2._1, tuple2Tuple2._2._2));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Integer, String> movies = sc.textFile(moviesFile).map(
                new Function<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(String s) throws Exception {
//                        Thread.sleep(100);
                        String[] pros = s.split(",");
                        return new Tuple2<>(Integer.parseInt(pros[1]), pros[2]);
                    }
                }
        ).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, String>>, Integer, String>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<Integer, String>> tuple2Iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<Integer, String> tuple2 = tuple2Iterator.next();
                    list.add(new Tuple2<>(tuple2._1, tuple2._2));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        });

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Double>, String>> join1 = (JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Double>, String>>) reducedRatings.join(movies);
        JavaPairRDD<Integer, String> joinMovies = join1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Double>, String>>>, Integer, String>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<Integer, Tuple2<Tuple2<Integer, Double>, String>>> tuple2Iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Thread.sleep(5);
                    Tuple2<Integer, Tuple2<Tuple2<Integer, Double>, String>> tuple = tuple2Iterator.next();
                    list.add(new Tuple2<>(tuple._1, tuple._2._2));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Integer, Tuple2<String, Double>> moviesRating = (JavaPairRDD<Integer, Tuple2<String, Double>>) joinMovies
                .join(moviesAndRating)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<String, Double>>>, Integer, Tuple2<String, Double>>() {
                    @Override
                    public Iterator<Tuple2<Integer, Tuple2<String, Double>>> call(Iterator<Tuple2<Integer, Tuple2<String, Double>>> tuple2Iterator) throws Exception {
                        List<Tuple2<Integer, Tuple2<String, Double>>> list = new ArrayList<>();
                        while (tuple2Iterator.hasNext()) {
//                            Thread.sleep(1);
                            Tuple2<Integer, Tuple2<String, Double>> tuple = tuple2Iterator.next();
                            list.add(tuple);
                        }
                        return list.iterator();
                    }
                });


        LOG.info("result is " + moviesRating.first()._1);
    }
}
