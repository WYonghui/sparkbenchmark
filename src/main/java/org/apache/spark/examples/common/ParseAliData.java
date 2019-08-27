package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/*
* 分析alibaba/clusterdata/cluster-trace-v2018/batch_task.csv中DAG型job的task数量和深度
* 说明：其中包含一些job存在环路，这种情况排除在外。还有一些task没有信息，但是被其他task依赖，这种task也在计算之中
* java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.ParseAliData \
    E:\download\ali-cluster-Data\batch_task.csv E:\download\ali-cluster-Data\result.csv
 */
public class ParseAliData {

    final static Logger log = LoggerFactory.getLogger(ParseAliData.class);
    private static Integer realTaskNum = 0;  //记录每个result task所在的DAG中task数量

    private Integer parentDepth(Map<String, ArrayList<String>> taskInfo, String task, Map<String, Integer> depthMap,
                                Integer times) {
        ArrayList<String> parentTasks = taskInfo.get(task);
        //递归出口，某些task未单独出现，所以未收集在taskInfo中
        if (parentTasks == null) {
            depthMap.put(task, 1);
            return 1;
        }

        //递归出口，无父task的root task
        if (parentTasks.size() == 0) {
            depthMap.put(task, 1);
            return 1;
        }

        if (times > 100) {//有些job存在环路，属异常情况
            return 2000;
        }

        Integer recordedDepth = depthMap.get(task);
        if (recordedDepth != null) {
            return recordedDepth;
        }

        Integer maxDepth = 0;
        Iterator<String> iterator = parentTasks.iterator();
        while (iterator.hasNext()) {
            String parentTask = iterator.next();
            ParseAliData.realTaskNum++;
            Integer parentDepth = parentDepth(taskInfo, parentTask, depthMap, times + 1);  //递归
            if (parentDepth > maxDepth) {
                maxDepth = parentDepth;
            }
        }

        depthMap.put(task, maxDepth + 1);
        return maxDepth + 1;
    }


    //分析给定job中task深度
    //返回值的第一个参数是task数量，第二个参数是task深度
    private Tuple2<Integer, Integer> calculatingDepth(Map<String, ArrayList<String>> jobInfo, String jobName) {
        Integer taskNum = jobInfo.get(jobName).size();  //当前job中task数量
        Map<String, ArrayList<String>> taskInfo = new HashMap<>();
        Iterator<String> taskIterator = jobInfo.get(jobName).iterator();
        Set<String> medianTasks = new HashSet<>(); //所有非resultTask的task
        Set<String> allTasks = new HashSet<>(); //所有的task，包含resultTask

        //对task逐个分析，拆分出当前task及其依赖的父task
        while (taskIterator.hasNext()) {
            String taskName = taskIterator.next();
            String[] strs = taskName.split("_");
            String curTask = strs[0].substring(1);

            allTasks.add(curTask);
            //将所有的父task加入到列表中。如果无父task，则列表为空
            ArrayList<String> parentTasks = new ArrayList<>();
            for (int i = 1; i < strs.length; i++) {
                parentTasks.add(strs[i]);
                medianTasks.add(strs[i]);
                allTasks.add(strs[i]);  //有些task只在父task中出现
            }
            taskInfo.put(curTask, parentTasks);
        }

        //只有一个task，深度为1
        if (allTasks.size() < 2) {
            return new Tuple2<>(allTasks.size(), allTasks.size());
        }

        Integer maxDepth = 0;
        Integer maxTaskNum = 0;
        //先判断出result task
        Iterator<String> iterator = allTasks.iterator();
        while (iterator.hasNext()) {
            String curTask = iterator.next();
            if (medianTasks.contains(curTask)) {
                continue;  //非result task
            }

            //result task
            //递归分析task深度
            Map<String, Integer> depthMap = new HashMap<>();
            ParseAliData.realTaskNum = 1;
            Integer depth = parentDepth(taskInfo, curTask, depthMap, 1);
            if (depth > maxDepth) {
                maxDepth = depth;
                maxTaskNum = realTaskNum;
            }

        }

        if (maxDepth > maxTaskNum) { //异常job存在环路
            maxDepth = -1;
        }

        return new Tuple2<>(maxTaskNum, maxDepth);
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            log.warn("Usage: java <mainClass> <srcFile> <resultFile>");
            System.exit(-1);
        }

        //文件路径由参数指定
        String srcFile = args[0];
        String result = args[1];
        Map<String, ArrayList<String>> jobInfo = new HashMap<>();
        ParseAliData parseAliData = new ParseAliData();

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(srcFile);

        } catch (FileNotFoundException e) {
            log.error(srcFile + "is not exist.");
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        // 按行读取文件内容并处理
        String str = null;
        try {
            //先提取task信息放入相应的arraylist中
            while ((str = reader.readLine()) != null) {
                String[] strs = str.split(",");
                String taskName = strs[0];
                String jobName = strs[2];

                //跳过tasks that are not DAGs
                if (taskName.contains("task_") || taskName.contains("MergeTask")) {
                    continue;
                }

                ArrayList<String> taskInfo = null;
                if ((taskInfo = jobInfo.get(jobName)) == null) {
                    taskInfo = new ArrayList<>();
                    jobInfo.put(jobName, taskInfo);
                }

                taskInfo.add(taskName);

            }
            log.info("完成task分组.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        reader.close();
        inputStream.close();

        //开始分析每个job的task数量和深度情况
        FileWriter resultWriter = new FileWriter(result);
        BufferedWriter writer = new BufferedWriter(resultWriter);
        Set<String> jobNames = jobInfo.keySet();
        Iterator<String> iterator = jobNames.iterator();

        log.info("The number of jobs is " + jobNames.size());

        //逐个分析job
        while (iterator.hasNext()) {
            String jobName = iterator.next();  //job ID
            Integer taskNum = jobInfo.get(jobName).size();  //当前job中task数量

            //分析job中task深度, "j_3734942" 有环路 j_1575128, j_2598590
            log.debug("Calculating the depth of job " + jobName + ", taskNum " + taskNum);
//            if (!jobName.equals("j_2215257")) continue;
            Tuple2<Integer, Integer> taskNumAndDepth = parseAliData.calculatingDepth(jobInfo, jobName);

            log.debug("Depth is " + taskNumAndDepth._2);

            if (taskNumAndDepth._2 < 0) {  //异常job存在环路
                log.info("Calculating the depth of job " + jobName + ", taskNum " + taskNum + ", Depth is " + taskNumAndDepth._2);
                continue; //跳过异常job
            }
            if (taskNumAndDepth._1 == 1) { //去掉非DAG型作业
                continue;
            }
            writer.write(jobName + "," + taskNumAndDepth._1 + "," + taskNumAndDepth._2);
            writer.newLine();
        }

        writer.flush();
        writer.close();
        resultWriter.close();

//        //初步写出到文件，查看情况
//        FileWriter writer = new FileWriter(result);
//        BufferedWriter bufferedWriter = new BufferedWriter(writer);
//        Set<String> jobNames = jobInfo.keySet();
//        Iterator<String> iterator = jobNames.iterator();
//        log.info("The number of jobs is " + jobNames.size());
//        bufferedWriter.write("total jobs: "+jobNames.size());
//        bufferedWriter.newLine();
//        while (iterator.hasNext()) {
//            String jobName = iterator.next();
//            bufferedWriter.write(jobName + "," + jobInfo.get(jobName).toString());
//            bufferedWriter.newLine();
//        }
//        bufferedWriter.flush();
//        bufferedWriter.close();
//        writer.close();
    }
}
