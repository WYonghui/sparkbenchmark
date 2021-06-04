package org.apache.spark.examples.aliTrace.MultistageEvaluation;

import org.apache.commons.cli.*;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.JobCommandParser;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.DAGScheduler;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.DAGSchedulerForTelescope;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;



/**
 * 测试每个job在不同权重下的执行时间
 * Submitting a job to the cluster, the scheduler proposes a scheduling plan and calculates the job completion time.
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.aliTrace.MultistageEvaluation.MultiJobCompletionTime \
     -s F:\telescope\测试\阿里巴巴数据集\随机选取1000作业\ -d F:\telescope\测试\阿里巴巴数据集\作业执行信息\completionTime.csv
 * @author yonghui
 * @date 2020-09-29
 */
public class MultiJobCompletionTime {
    private static final Logger logger = LoggerFactory.getLogger(MultiJobCompletionTime.class);

    public static void main(String[] args) throws Exception{
        //两个输入参数：源文件目录，输出文件
        Options options = new Options();
        Option op1 = new Option("s", "sourcePath", true, "source path");
        op1.setRequired(true);
        Option op2 = new Option("d", "dstFile", true, "output file");
        op2.setRequired(true);
        options.addOption(op1);
        options.addOption(op2);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException ex) {
            HelpFormatter helper = new HelpFormatter();
            helper.printHelp("MultiJobCompletionTime -s <srcPath> -d <dstFile>", options);
            System.exit(-1);
        }

        String srcPath = commandLine.getOptionValue('s');
        String dstFile = commandLine.getOptionValue('d');
        FileOutputStream outputStream = new FileOutputStream(dstFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

        long startTime = System.currentTimeMillis();

        File path = new File(srcPath);
        for (String fileName: path.list()) {
            String file = srcPath + "\\" + fileName;

            logger.info(fileName);
            StringBuilder builder = new StringBuilder(fileName + ",");
//            writer.write(fileName + ": ");


            int runTime = 0;
            boolean hasDifference = false;
            double weight = 0;
            while (weight <= 1) {
                SubmittingJob submit = new SubmittingJob("telescope", 480, 600, weight);
                submit.init(file);
                int jct = submit.submitJob();
                builder.append(jct + ",");
//                writer.write(jct + ",");
                if (jct != runTime) {
                    if (runTime != 0) {
                        hasDifference = true;
                    }
                    runTime = jct;

                }

                weight += 0.003;
            }

            if (hasDifference){
                writer.write(builder.toString());
                writer.newLine();

            }

        }

        writer.flush();
        writer.close();
        outputStream.close();

        long endTime = System.currentTimeMillis();
        logger.info("Run time is {} ms.", (endTime - startTime));

    }
}
