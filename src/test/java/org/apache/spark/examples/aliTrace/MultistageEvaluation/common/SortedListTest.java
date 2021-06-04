package org.apache.spark.examples.aliTrace.MultistageEvaluation.common; 

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;

/** 
* SortedList Tester. 
* 
* @author yonghui 
* @since 09/30/2020 
* @version 1.0 
*/ 
public class SortedListTest { 
    private static final Logger logger = LoggerFactory.getLogger(SortedListTest.class);

    @Test
    public static void main(String[] args) {
        // Test goes here...
        SortedList<String, String> list = new SortedList<>();
        list.add("1", "a");
        list.add("3", "11");
        list.add(new SortedList.Entry<>("2", "c"));
        list.add(new SortedList.Entry<>("4", "a"));

        logger.info("list contains 2: {}", list.contains(new SortedList.Entry<String, String>("2", "c")));
        logger.info("list containsKey 2: {}", list.containsId("2"));

        Iterator<SortedList.Entry<String, String>> iterator = list.iterator();
        while (iterator.hasNext()) {
            SortedList.Entry<String, String> entry = iterator.next();
            logger.info("{}, {}", entry.id, entry.value);
        }

        Random random = new Random(515);
        for (int i = 0; i < 100; i++) {
            logger.info("{}", random.nextInt(20) + 3);
        }

    }
    

} 
