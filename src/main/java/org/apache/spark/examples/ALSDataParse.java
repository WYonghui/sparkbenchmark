package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;


/**
 * hdfs dfs -rm -r -f /wyh/dataset/ALS/ml-100k/alsTest.data
 * spark-submit --master spark://node91:6066 --deploy-mode cluster --class org.apache.spark.examples.ALSDataParse \
 spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar \
 hdfs://node91:9000/wyh/dataset/ALS/ml-100k/u.data hdfs://node91:9000/wyh/dataset/ALS/ml-100k
 */
public class ALSDataParse {

    private static final Logger log = LoggerFactory.getLogger(ALSDataParse.class);

    private static class ParseRating implements Function<String, Rating> {
        private final Pattern TAB = Pattern.compile("\t");

        @Override
        public Rating call(String s) throws Exception {
            String[] strs = TAB.split(s);
            Rating rating = new Rating(Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Double.parseDouble(strs[2]));
            return rating;
        }
    }


    /**
     * @param uData
     * @return JavaRDD
     */
    private JavaRDD<Rating> prepareData(JavaRDD<String> uData) {

        //读取评分数据并解析,
        // 这种写法出现错误，task not serializable
//        JavaRDD<Rating> ratingRDD = uData.map(new Function<String, Rating>() {
//            @Override
//            public Rating call(String s) throws Exception {
//                String[] strs = s.split("\t");
//                Rating rating = new Rating(Integer.parseInt(strs[0]), Integer.parseInt(strs[1]), Double.parseDouble(strs[2]));
//                return rating;
//            }
//        });
        JavaRDD<Rating> ratingRDD = uData.map(new ParseRating());
        log.info("----------完成解析评分数据-------------");

        //数据初步统计
//        Long ratingNum = ratingRDD.count(); //评价条数
//        Long usersNum = ratingRDD.map((Rating rating) -> {
//
//            return rating.user();
//        }).distinct().count(); //用户数
//        Long moviesNum = ratingRDD.map((Rating rating) -> {
//
//            return rating.product();
//        }).distinct().count();
//        log.info("Total ratings: " + ratingNum + ", user number: " + usersNum + ", movie number: " + moviesNum);

        return ratingRDD;
    }


    /**
     * main方法，主程序入口
     *
     * @param args
     */
    public static void main(String[] args) {

        ALSDataParse alsTest = new ALSDataParse();
        SparkSession ss = SparkSession
                .builder()
                .appName("ALSDataParse")
                .getOrCreate();

        JavaRDD<String> uData = ss.read().textFile(args[0]).javaRDD();

        log.info("----------------数据准备-----------------");
        JavaRDD<Rating> ratingRDD = alsTest.prepareData(uData);


        //保存中间数据
        String dstPath = args[1] + "/alsTest.data";
        ratingRDD.map((Rating rating) -> {
            return rating.user() + "," + rating.product() + "," + rating.rating();
        }).saveAsTextFile(dstPath);

        ss.stop();
    }


}

//ml-100k/u.data: Total ratings: 100000, user number: 943, movie number: 1682