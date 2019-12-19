package org.apache.spark.examples.aliTrace;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.aliTrace.Job1077388 \
       spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Job1077388 15 ratings.csv tags.csv 0
 */
public class Job1077388 {

    private static Logger LOG = LoggerFactory.getLogger(Job1077388.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            LOG.error("Usage: Job1077388 <appName> <parallelism> <inputFile1> <inputFile2>");
            System.exit(-1);
        }

        SparkSession.Builder sparkBuilder = SparkSession
                .builder()
                .appName(args[0])
                .config("spark.default.parallelism", Integer.parseInt(args[1]));

        if (args.length == 5) {
            sparkBuilder.config("spark.locality.wait", Integer.parseInt(args[4]));
        }

        SparkSession spark = sparkBuilder.getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String ratingsFile = args[2];
        String tagsFile = args[3];

        JavaPairRDD<Integer, Double> ratings = sc.textFile(ratingsFile)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Double>() {
                    @Override
                    public Iterator<Tuple2<Integer, Double>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(8000);
                        List<Tuple2<Integer, Double>> list = new ArrayList<>();
                        if (stringIterator.hasNext()){
                            String str = stringIterator.next();
                            String[] pros = str.split(",");
                            list.add(new Tuple2<>(Integer.parseInt(pros[1]), Double.parseDouble(pros[2])));
                        }

                        return list.iterator();
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double s, Double s2) throws Exception {
                        return (s + s2) / 2;
                    }
                }); // single stage

        JavaPairRDD<Integer, Tuple2<Integer, String>> tags = sc.textFile(tagsFile)
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Tuple2<Integer, String>>() {
                    @Override
                    public Iterator<Tuple2<Integer, Tuple2<Integer, String>>> call(Iterator<String> stringIterator) throws Exception {
                        Thread.sleep(14000);
                        List<Tuple2<Integer, Tuple2<Integer, String>>> list = new ArrayList<>();
                        if (stringIterator.hasNext()){
                            String str = stringIterator.next();
                            String[] pros = str.split(",");
                            list.add(new Tuple2<>(Integer.parseInt(pros[1]), new Tuple2<>(Integer.parseInt(pros[0]), pros[2])));
                        }
                        return list.iterator();
                    }
                }).reduceByKey(new Function2<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> integerStringTuple22) throws Exception {
                        return integerStringTuple2;
                    }
                }).cache();

        JavaPairRDD<Integer, Integer> movieAndUserId = tags
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, String>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, String>>> tuple2Iterator) throws Exception {
                        Thread.sleep(5000);
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        if (tuple2Iterator.hasNext()) {
                            Tuple2<Integer, Tuple2<Integer, String>> tuple2 = tuple2Iterator.next();
                            list.add(new Tuple2<>(tuple2._1, tuple2._2._1));
                        }
                        return list.iterator();
                    }
                });

        JavaPairRDD<Integer, Integer> movieAndTag = tags
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, String>>>, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Tuple2<Integer, String>>> tuple2Iterator) throws Exception {
                        Thread.sleep(6000);
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        if (tuple2Iterator.hasNext()) {
                            Tuple2<Integer, Tuple2<Integer, String>> tuple2 = tuple2Iterator.next();
                            list.add(new Tuple2<>(tuple2._1, 222));
                        }
                        return list.iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer;
                    }
                });

        JavaPairRDD<Integer, Tuple2<Double, Integer>> join = (JavaPairRDD<Integer, Tuple2<Double, Integer>>) ratings.join(movieAndUserId);
        JavaPairRDD<Integer, Integer> union = join
                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Double, Integer>> integerTuple2Tuple2) throws Exception {
                        return new Tuple2<>(integerTuple2Tuple2._1, integerTuple2Tuple2._2._2);
                    }
                })
                .union(movieAndTag);
        JavaRDD<Tuple2<Integer, Integer>> result = union
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                        Thread.sleep(500);
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        if (tuple2Iterator.hasNext()) {
                            Tuple2<Integer, Integer> tuple2 = tuple2Iterator.next();
                            list.add(new Tuple2<>(tuple2._1, tuple2._2));
                        }
                        return list.iterator();
                    }
                });

        result.collect();
    }


}
