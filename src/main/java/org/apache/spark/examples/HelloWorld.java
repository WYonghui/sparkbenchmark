package org.apache.spark.examples;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.HelloWorld -n helloWorld
 */
public class HelloWorld {
    private static final Logger LOG = LoggerFactory.getLogger(ScalaHelloWorld.class);

    @Argument(alias = "n", description = "application name") //, required = true
    private static String name;

    public static void main(String[] args) {
        String str = "this=is a str_string.";

        String[] strs = str.split("\\W+");
        for (int i = 0; i < strs.length; i++) {
            LOG.info(strs[i]);
        }
        List<String> unparsed = Args.parseOrExit(ScalaHelloWorld.class, args);

        LOG.info("application name: " + name);

        //需要设置mllib依赖的作用范围为compile
        Rating rating = new Rating(100, 231, 4.9);
        Integer userId = rating.user();
        Rating newRating = rating.copy(rating.copy$default$1(), rating.copy$default$2(), rating.copy$default$3());
        Rating new2Rating = rating.copy(rating.user(), rating.product(), rating.rating());

        LOG.info("rating's userID: " + userId);
        LOG.info("rating equal newRating: " + rating.equals(newRating));
        LOG.info("rating == newRating: " + (rating==(newRating)));

        LOG.info("rating equal new2Rating: " + rating.equals(new2Rating));
        LOG.info("rating == new2Rating: " + (rating==(new2Rating)));
    }
}
