package org.apache.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


/**
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.Job1077229 \
      spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1077229
 */
public class Job1077229 {
    private static Logger LOG = LoggerFactory.getLogger(Job1077229.class);


    public static void main(String[] args) {

        if (args.length < 1) {
            LOG.error("Usage: Job1077229 <appName>");
            System.exit(-1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", 12)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Tuple2<String, Integer>> moviesList = new ArrayList<>();
        addMoviesToList(moviesList);

        JavaPairRDD<String, Integer> movies = sc.parallelizePairs(moviesList);
        JavaPairRDD<String, Integer> moviesLat = movies.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Thread.sleep(2000);
                return stringIntegerTuple2;
            }
        }); //stage 1

        List<Tuple2<String, Double>> ratingsList = new ArrayList<>();
        addRatingsToList(ratingsList);
        JavaPairRDD<String, Double> ratings = sc.parallelizePairs(ratingsList);

        JavaPairRDD<String, Double> ratingsWithNoRepeatLat = ratings.mapToPair(
                new PairFunction<Tuple2<String, Double>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                Thread.sleep(2000);
                return stringDoubleTuple2;
            }
        }); // stage 2
//        ratingsWithNoRepeatLat.cache();


        JavaPairRDD<String, Tuple2<Integer, Double>> moviesWithRating = (JavaPairRDD<String, Tuple2<Integer, Double>>) moviesLat.join(ratingsWithNoRepeatLat);
        JavaPairRDD<String, Integer> moviesWithId = moviesWithRating.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Double>> stringTuple2Tuple2) throws Exception {
                Thread.sleep(2000);
                return new Tuple2<>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._1);
            }
        }); //stage 3

        JavaPairRDD<String, Double> ratingsEveryMovie = ratingsWithNoRepeatLat.reduceByKey((d1, d2) -> (d1 + d2) / 2);
        JavaPairRDD<String, Double> ratingsEveryMovieLat = ratingsEveryMovie.mapToPair(
                new PairFunction<Tuple2<String, Double>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                Thread.sleep(2000);
                return stringDoubleTuple2;
            }
        }); // stage4

        JavaPairRDD<String, Tuple2<Integer, Double>> result = (JavaPairRDD<String, Tuple2<Integer, Double>>) moviesWithId.join(ratingsEveryMovieLat);
        Long resultNum = result.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<String, Tuple2<Integer, Double>> stringTuple2Tuple2) throws Exception {
                Thread.sleep(1000);
                return stringTuple2Tuple2._2;
            }
        }).count();  //stage5
        LOG.info(resultNum.toString());
//        ratingsWithNoRepeatLat.unpersist();

        spark.stop();

    }

    private static void addMoviesToList(List<Tuple2<String, Integer>> list) {
        list.add(new Tuple2<>("Toy Story", 1));
        list.add(new Tuple2<>("Jumanji", 2));
        list.add(new Tuple2<>("Grumpier Old Men", 3));
        list.add(new Tuple2<>("Waiting to Exhale", 4));
        list.add(new Tuple2<>("Father of the Bride Part II", 5));
        list.add(new Tuple2<>("Heat", 6));
        list.add(new Tuple2<>("Sabrina", 7));
        list.add(new Tuple2<>("Tom and Huck", 8));
        list.add(new Tuple2<>("Sudden Death", 9));
        list.add(new Tuple2<>("GoldenEye", 10));
        list.add(new Tuple2<>("American President", 11));
        list.add(new Tuple2<>("Dracula: Dead and Loving It", 12));
    }

    private static void addRatingsToList(List<Tuple2<String, Double>> list) {
        list.add(new Tuple2<>("Toy Story", 3D));
        list.add(new Tuple2<>("Jumanji", 2D));
        list.add(new Tuple2<>("Grumpier Old Men", 2D));
//        list.add(new Tuple2<>("Grumpier Old Men", 4D));
        list.add(new Tuple2<>("Waiting to Exhale", 4D));
        list.add(new Tuple2<>("Father of the Bride Part II", 5D));
        list.add(new Tuple2<>("Heat", 3D));
        list.add(new Tuple2<>("Sabrina", 3D));
//        list.add(new Tuple2<>("Sabrina", 5D));
        list.add(new Tuple2<>("Tom and Huck", 2D));
        list.add(new Tuple2<>("Sudden Death", 1D));
        list.add(new Tuple2<>("GoldenEye", 5D));
        list.add(new Tuple2<>("American President", 4D));
        list.add(new Tuple2<>("Dracula: Dead and Loving It", 2D));
    }
}
