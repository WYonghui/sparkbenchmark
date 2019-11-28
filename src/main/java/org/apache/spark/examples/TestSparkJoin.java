package org.apache.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.TestSparkJoin \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
public class TestSparkJoin {
    private static Logger LOG = LoggerFactory.getLogger(TestSparkJoin.class);

    public static void main(String[] args) {

        SparkSession ss = SparkSession
                .builder()
                .appName("TestSparkJoin")
                .config("spark.default.parallelism", 3).getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        List<Tuple2<String, Integer>> moviesList = new ArrayList<>();
        moviesList.add(new Tuple2<>("Toy Story", 1));
        moviesList.add(new Tuple2<>("Grumpier Old Men", 3));
        moviesList.add(new Tuple2<>("Sabrina", 7));
        JavaPairRDD<String, Integer> movies = sc.parallelizePairs(moviesList);

        movies.cache();


//        List<Tuple2<String, Integer>> ratingList = new ArrayList<>();
//        ratingList.add(new Tuple2<>("Toy Story", 4));
//        ratingList.add(new Tuple2<>("Grumpier Old Men", 3));
//        ratingList.add(new Tuple2<>("Sabrina", 1));
//        JavaPairRDD<String, Integer> ratings = sc.parallelizePairs(ratingList);

//        JavaPairRDD<String, Tuple2<Integer, Integer>> join = (JavaPairRDD<String, Tuple2<Integer, Integer>>) movies.join(ratings);
        JavaPairRDD<String, Integer> moviesForJoin = movies.mapToPair(t2 -> new Tuple2<>(t2._1, t2._2)).reduceByKey((v1, v2) -> v1);
//        JavaPairRDD<String, Integer> ratingsForJoin = ratings.reduceByKey((v1, v2) -> v1);
//        JavaPairRDD<String, Integer> join = moviesForJoin.union(ratingsForJoin);

        JavaPairRDD<String, Integer> moviesReduceByKey = movies.mapToPair(t2 -> new Tuple2<>(t2._1, t2._2)).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                Thread.sleep(2000);
                return integer;
            }
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> join2 = (JavaPairRDD<String, Tuple2<Integer, Integer>>) moviesReduceByKey
                .join(moviesForJoin);

        Iterator<Tuple2<String, Tuple2<Integer, Integer>>> resultIterator = join2.collect().iterator();
        LOG.info("Printing the result. =======================================");
        while (resultIterator.hasNext()) {
            LOG.info(resultIterator.next().toString());
        }
        LOG.info("=============================================================");
//        movies.unpersist();
        ss.stop();
    }
}
