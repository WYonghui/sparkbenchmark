package org.apache.spark.examples.aliTrace.MultistageEvaluation.common; 

import org.apache.commons.cli.CommandLine;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
* JobCommandParser Tester. 
* 
* @author yonghui
* @since 09/29/2020 
* @version 1.0 
*/ 
public class JobCommandParserTest { 
    private static final Logger logger = LoggerFactory.getLogger(JobCommandParser.class);

    @Test
    public static void main(String[] args) {
        //TODO: Test goes here... 
        JobCommandParser parser = new JobCommandParser("JobCommandParserTest");
        String[] strs = new String[]{"-s", "spark", "-j", "/opt/test", "-p", "12", "-c", "8"};
//        String[] strs = new String[]{"-s", "spark", "-j", "/opt/test", "-p", "12"}; //error test
        CommandLine commandLine = parser.parse(strs);

        if (commandLine.hasOption('s')) {
            logger.info("s: {}", commandLine.getOptionValue('s'));
        }
    }
    

} 
