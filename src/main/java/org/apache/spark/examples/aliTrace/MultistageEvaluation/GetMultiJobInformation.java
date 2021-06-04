package org.apache.spark.examples.aliTrace.MultistageEvaluation;

import org.apache.commons.cli.*;
import org.apache.spark.examples.common.ParseVertexAndEdgeNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/**
 * 从alibaba trace中获取指定job的dag信息
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.aliTrace.MultistageEvaluation.GetJobInformation \
 -s F:\telescope\测试\阿里巴巴数据集\batch_task.csv -d F:\telescope\测试\阿里巴巴数据集\指定job的信息\ -j j_1981074 -n 10
 * @author yonghui
 * @since 2020-09-29
 * @description
 */
public class GetMultiJobInformation {
    private static final Logger logger = LoggerFactory.getLogger(GetMultiJobInformation.class);


    private Map<String, ArrayList<String>> loadFile(String src) throws IOException{
        Map<String, ArrayList<String>> jobInfo = new HashMap<>();
        FileInputStream inputStream = new FileInputStream(src);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        // 按行读取文件内容并处理
        // 先提取task信息放入相应的ArrayList中
        String str = null;
        while ((str = reader.readLine()) != null) {
            String[] strs = str.split(",");
            String taskName = strs[0];
            String jobName = strs[2];

            //跳过非常规任务信息
            if (taskName.contains("task_") || taskName.contains("MergeTask")) {
                continue;
            }

            ArrayList<String> taskInfo = taskInfo = jobInfo.get(jobName);
            if (taskInfo == null) {
                taskInfo = new ArrayList<>();
                jobInfo.put(jobName, taskInfo);
            }

            taskInfo.add(taskName);

        }
        reader.close();
        inputStream.close();
        logger.info("Complete loading file.");

        return jobInfo;
    }




    /**
     * @param args args[0] 源文件，args[1] 输出文件目录
     */
    public static void main(String[] args) throws Exception{
        //两个输入参数：源文件，输出文件目录
        Options options = new Options();
        Option op1 = new Option("s", "sourceFile", true, "alibaba trace file");
        op1.setRequired(true);
        Option op2 = new Option("d", "dstPath", true, "output path");
        op2.setRequired(true);
        options.addOption(op1);
        options.addOption(op2);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException ex) {
            HelpFormatter helper = new HelpFormatter();
            helper.printHelp("GetMultiJobInformation -s <srcFile> -d <dstPath>", options);
            System.exit(-1);
        }

        GetMultiJobInformation getJobs = new GetMultiJobInformation();
        String src = commandLine.getOptionValue('s');
        String dstPath = commandLine.getOptionValue('d');


        Map<String, ArrayList<String>> jobInfo = getJobs.loadFile(src);
        ParseVertexAndEdgeNumber parseAliData = new ParseVertexAndEdgeNumber();

        // 从作业集中随机选取1000个作业，输出到以作业名称为文件名的文件中
        int outputJobNumber = 0;
        Random random = new Random(System.currentTimeMillis());
        Iterator<String> iterator = jobInfo.keySet().iterator();
        while (iterator.hasNext() && outputJobNumber < 500) {
            String jobName = iterator.next();

            Tuple2<Integer, Integer> stageAndEdgeNumber = parseAliData.calculatingStagesAndEdges(jobInfo, jobName);
            String file = dstPath + stageAndEdgeNumber._1 + "-" + stageAndEdgeNumber._2 + "-" + jobName + ".csv";

            // 只选取阶段数大于20的作业
            if (stageAndEdgeNumber._1 > 20 && stageAndEdgeNumber._2 > stageAndEdgeNumber._1  && random.nextInt(2000) <= 1000) {
                FileOutputStream outputStream = new FileOutputStream(file);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

                List<String> stages = jobInfo.get(jobName);
                for (String stage: stages) {
                    writer.write(stage);
                    writer.newLine();
                }

                writer.flush();
                outputStream.close();
                writer.close();
                outputJobNumber++;
            }

        }
        logger.info("Succeed to select 1000 jobs.");

    }

}
