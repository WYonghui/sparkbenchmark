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
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.WordCount \
     --name WordCount spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
     -file hdfs://node91:9000//wikipedia_30GB/file_7* \
     -parallelism 28 -wait 0
 */
public class WordCount {
    private static Logger LOG = LoggerFactory.getLogger(WordCount.class);
    private static final Pattern SPACE = Pattern.compile("\\W+");

    @Argument(alias = "f", description = "source file", required = true)
    private static String file;

    @Argument(alias = "p", description = "default parallelism")
    private static String parallelism;

    @Argument(alias = "w", description = "delay waiting time")
    private static String wait;

    public static void main(String[] args) {

        SparkSession.Builder builder = SparkSession.builder();
        //解析参数
        Args.parseOrExit(WordCount.class, args);
        builder.appName("WordCount");
        if (parallelism != null)                   //设置并行度
            builder.config("spark.default.parallelism", parallelism);
        if (wait != null)                          //设置延迟调度等待时间
            builder.config("spark.locality.wait", wait);

        SparkSession spark = builder.getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(file).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        counts.saveAsTextFile("hdfs://node39:9000/hadoop-5nodes/output/wordCount/0001");

        spark.stop();

    }
}
