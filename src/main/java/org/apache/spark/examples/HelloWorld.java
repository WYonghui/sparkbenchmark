package org.apache.spark.examples;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.HelloWorld -n helloWorld
 */
public class HelloWorld {
    private static final Logger LOG = LoggerFactory.getLogger(HelloWorld.class);

//    @Argument(alias = "n", description = "application name") //, required = true
//    private static String name;

    public static void main(String[] args) throws Exception{
//        String str = "this=is a str_string.";
//
//        String[] strs = str.split("\\W+");
//        for (int i = 0; i < strs.length; i++) {
//            LOG.info(strs[i]);
//        }
//        List<String> unparsed = Args.parseOrExit(ScalaHelloWorld.class, args);
//
//        LOG.info("application name: " + name);
//
//        //需要设置mllib依赖的作用范围为compile
//        Rating rating = new Rating(100, 231, 4.9);
//        Integer userId = rating.user();
//        Rating newRating = rating.copy(rating.copy$default$1(), rating.copy$default$2(), rating.copy$default$3());
//        Rating new2Rating = rating.copy(rating.user(), rating.product(), rating.rating());
//
//        LOG.info("rating's userID: " + userId);
//        LOG.info("rating equal newRating: " + rating.equals(newRating));
//        LOG.info("rating == newRating: " + (rating==(newRating)));
//
//        LOG.info("rating equal new2Rating: " + rating.equals(new2Rating));
//        LOG.info("rating == new2Rating: " + (rating==(new2Rating)));

//        测试使用浮点乘法占用CPU
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置日期格式
////        String start = df.format(System.currentTimeMillis());
//        Long start = System.currentTimeMillis();
//        LOG.info(df.format(start));
//        while (true) {
//            double a = 12332.234;
//            double b = 23545342.2342;
//            double c = a * b;
//            if ((System.currentTimeMillis() - start) > 50000) {
//                break;
//            }
//        }
//
//        String end = df.format(new Date());
//        LOG.info(end);
//        String end2 = df.format(System.currentTimeMillis());
//        LOG.info(end2);
//        LOG.info((Long.parseLong(end) - Long.parseLong(start)) + "");


//    test Set
//        String path = "C:\\Users\\10564\\Downloads\\enwiki-2018-hc-t.graph";
//        FileInputStream inputStream = new FileInputStream(path);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//        String row;
//        int line = 0;
//        while ((row = reader.readLine()) != null) {
//            LOG.info(row);
//            line++;
//            if (line >= 10) {
//                break;
//            }
//        }
//
//        reader.close();
//        inputStream.close();

//        test rand function
//        Random random = new Random(527);
//
        for (int i = 0; i < 60; i++) {
            System.out.println(random.nextInt(50) +5);
        }


//        test possion Probability
//        for (int i = 0; i < 50; i++) {
//            System.out.println(getPossionVariable(8));
//        }
    }

    private static Random random = new Random(516);

    private static int getPossionVariable(double lamda) {
        int x = 0;
//        double y = Math.random();
        double y = random.nextDouble();
        double cdf = getPossionProbability(x, lamda);
        while (cdf < y) {
            x++;
            cdf += getPossionProbability(x, lamda);
        }
        return x;
    }

    private static double getPossionProbability(int k, double lamda) {
        double c = Math.exp(-lamda), sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= lamda / i;
        }
        return sum * c;
    }

}
