package org.apache.spark.examples;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * hdfs dfs -rm -r -f hdfs://node91:9000/wyh/output/wordcount/*
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.WordFilterAndCount \
 spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar -name WordFilterAndCount \
 -file hdfs://node91:9000/wyh/testDataSet/wikipedia_30GB -parallelism 800 -wait 0
 */
public class WordFilterAndCount {
    private static Logger LOG = LoggerFactory.getLogger(WordFilterAndCount.class);
    private static final Pattern SPACE = Pattern.compile("\\W+");

    @Argument(alias = "f", description = "source file", required = true)
    private static String file;

    @Argument(alias = "n", description = "application name", required = true)
    private static String name;

    @Argument(alias = "p", description = "default parallelism")
    private static String parallelism;

    @Argument(alias = "w", description = "delay waiting time")
    private static String wait;

    public static void main(String[] args) {
//        if (args.length < 1) {
//            LOG.error("Usage: WordFilterAndCount <srcFile>");
//            System.exit(-1);
//        }
//
//        SparkSession ss = SparkSession
//                .builder()
//                .appName("WordFilterAndCount")
//                .getOrCreate();

        SparkSession.Builder builder = SparkSession.builder();

        //解析参数
        Args.parseOrExit(WordFilterAndCount.class, args);
        builder.appName(name);
        if (parallelism != null)                   //设置并行度
            builder.config("spark.default.parallelism", parallelism);
        if (wait != null)                          //设置延迟调度等待时间
            builder.config("spark.locality.wait", wait);

        SparkSession ss = builder.getOrCreate();

        JavaRDD<String> lines = ss.read().textFile(file).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaRDD<String> filterWords = words.sample(true, 0.95);
        JavaRDD<String> repartition = filterWords.repartition(Integer.parseInt(parallelism));

        JavaPairRDD<String, Integer> ones = repartition.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//        counts.saveAsTextFile("hdfs://node91:9000//wyh/output/wordcount/001");

        JavaRDD<String> hyperlink = lines.filter(s -> s.contains("href"));

        JavaRDD<String> words2 = hyperlink.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones2 = words2.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts2 = ones2.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<String, Integer> result = counts.union(counts2);
        result.saveAsTextFile("hdfs://node91:9000//wyh/output/wordcount/001");

        ss.stop();

    }

}
