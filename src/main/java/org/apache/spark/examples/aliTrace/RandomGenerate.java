package org.apache.spark.examples.aliTrace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RandomGenerate {
    private static final Logger logger = LoggerFactory.getLogger("RandomGenerate");

    public static void main(String[] args) {
        Random rand = new Random(1024);

        for (int i = 0; i < 10; i++) {
            logger.info("No.{}:\t{}", i + 1, rand.nextInt(10) + 1);
        }

    }

}
