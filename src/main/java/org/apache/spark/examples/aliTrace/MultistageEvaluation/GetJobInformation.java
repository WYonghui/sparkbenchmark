package org.apache.spark.examples.aliTrace.MultistageEvaluation;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * 从alibaba trace中获取指定job的dag信息
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.aliTrace.MultistageEvaluation.GetJobInformation \
 -s F:\telescope\测试\阿里巴巴数据集\batch_task.csv -d F:\telescope\测试\阿里巴巴数据集\指定job的信息\ -j j_1981074 -n 10
 * @author yonghui
 * @since 2020-09-29
 * @description
 */
public class GetJobInformation {
    private static final Logger logger = LoggerFactory.getLogger(GetJobInformation.class);

    public static void main(String[] args) {
        //三个输入参数：源文件，输出文件目录，作业id, 作业中stage数量
        Options options = new Options();
        Option op1 = new Option("s", "sourceFile", true, "alibaba trace file");
        op1.setRequired(true);
        Option op2 = new Option("d", "dstFile", true, "output path");
        op2.setRequired(true);
        Option op3 = new Option("j", "jobId", true, "job id");
        op3.setRequired(true);
        Option op4=  new Option("n", "stageNum", true, "stage number");
        op4.setRequired(true);
        options.addOption(op1);
        options.addOption(op2);
        options.addOption(op3);
        options.addOption(op4);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException ex) {
            HelpFormatter helper = new HelpFormatter();
            helper.printHelp("GetJobInformation -s <srcFile> -d <dstFile> -j <jobId> -n <stageNum>", options);
            System.exit(-1);
        }

        String src = commandLine.getOptionValue('s');
        String dstPath = commandLine.getOptionValue('d');
        String jobId = commandLine.getOptionValue('j');
        String stageNum = commandLine.getOptionValue('n');
        String dst = dstPath + jobId + "-" + stageNum + ".csv";

        FileInputStream inputStream = null;
        FileOutputStream outputStream = null;
        try {
            inputStream = new FileInputStream(src);
            outputStream = new FileOutputStream(dst);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        // 按行读取文件内容并处理
        String str = null;
        try {
            // 过滤出指定job的task信息
            while ((str = reader.readLine()) != null) {
                String[] strs = str.split(",");
                String taskName = strs[0];
                String jobName = strs[2];

                if (jobName.equals(jobId)) {
                    writer.write(taskName);
                    writer.newLine();
                }
            }
            logger.info("Output all the information of {}", jobId);

            reader.close();
            inputStream.close();
            writer.flush();
            writer.close();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
