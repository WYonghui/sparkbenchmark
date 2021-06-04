package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/*
* 分析alibaba/clusterdata/cluster-trace-v2018/batch_task.csv中DAG型job的stage数量和边数量
* 说明：其中包含一些job存在环路，这种情况排除在外。还有一些task没有信息，但是被其他task依赖，这种task也在计算之中
* java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.ParseVertexAndEdgeNumber \
    E:\download\ali-cluster-Data\batch_task.csv E:\download\ali-cluster-Data\result.csv
 */
public class ParseVertexAndEdgeNumber {

    final static Logger log = LoggerFactory.getLogger(ParseVertexAndEdgeNumber.class);

    //返回值的第一个参数是task数量，第二个参数是task之间边数
    public Tuple2<Integer, Integer> calculatingStagesAndEdges(Map<String, ArrayList<String>> jobInfo, String jobName) {
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

        Integer maxTaskNum = 0;
        Integer maxEdgeNum = 0;
        Iterator<String> iterator = allTasks.iterator();
        //先判断出result task，有可能存在多个result task，找包含task最多的一个，计算task数量和edge数量
        while (iterator.hasNext()) {
            String curTask = iterator.next();
            if (medianTasks.contains(curTask)) {
                continue;  //非result task
            }

            // curTask是一个result task
            // 通过dfs对以curTask为result task的job进行遍历
            // 遍历过程中使用state标记task的遍历状态，0表示dfs未访问，-1表示当前dfs已访问, 1表示其他dfs已访问
            Deque<String> stack = new ArrayDeque<>();
            Map<String, Integer> taskState = new HashMap<>();
            stack.push(curTask);
            Integer taskNum = 0;
            Integer edgeNum = 0;
            while (!stack.isEmpty()) {
                String task = stack.peek();
                if (taskState.containsKey(task)) {
                    stack.pop();
                    taskState.put(task, 1);
                    continue;
                } else {
                    taskState.put(task, -1); //将task状态设置为当前dfs已访问
                }

                if (!taskInfo.containsKey(task) || (taskInfo.get(task).size() == 0)) { //源task，无父task
                    stack.pop();
                    taskState.put(task, 1);  //回溯，将该task状态设置为dfs已访问
                    continue;
                }

                List<String> parents = taskInfo.get(task);
                edgeNum += parents.size();
                boolean flag = false;
                for (String parent: parents) {
                    if (!taskState.containsKey(parent)) { //将未访问过的父节点加入到栈中
                        stack.push(parent);
                    } else if (taskInfo.containsKey(parent) && taskState.get(parent) == -1) { // 标识环路
                        flag = true;
                        break;
                    }
                }
                if (flag) { //存在环路
                    taskState.clear();
                    break;
                }

            }

            if (taskState.isEmpty()) { //存在环路
                break;
            }
            taskNum = taskState.keySet().size();
            maxTaskNum = Math.max(maxTaskNum, taskNum);
            maxEdgeNum = Math.max(maxEdgeNum, edgeNum);

        }

        return new Tuple2<>(maxTaskNum, maxEdgeNum);
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
        ParseVertexAndEdgeNumber parseAliData = new ParseVertexAndEdgeNumber();

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

        //开始分析每个job的stage数量和edge情况
        FileWriter resultWriter = new FileWriter(result);
        BufferedWriter writer = new BufferedWriter(resultWriter);
        Set<String> jobNames = jobInfo.keySet();
        Iterator<String> iterator = jobNames.iterator();

        long totalStages = 0;
        long totalEdges = 0;
        long totalJobs = 0;
        //逐个分析job
        while (iterator.hasNext()) {
            String jobName = iterator.next();  //job ID
//            Integer taskNum = jobInfo.get(jobName).size();  //当前job中task数量

//            if (!jobName.equals("j_319986")) continue;

            //分析job中task数量, "j_3734942" 有环路 j_1575128, j_2598590
            log.debug("Calculating the stage and edge number of job " + jobName);
            Tuple2<Integer, Integer> stageAndEdgeNumber = parseAliData.calculatingStagesAndEdges(jobInfo, jobName);

            //去掉非DAG型作业（只有一个stage或者具有环路）
            if (stageAndEdgeNumber._1 <= 1) {
                continue;
            }

            totalStages += stageAndEdgeNumber._1;
            totalEdges += stageAndEdgeNumber._2;
            totalJobs++;
            //统计阶段数在20 - 25之间的作业
            if (stageAndEdgeNumber._1 >= 75 && stageAndEdgeNumber._1 <= 80) {
                writer.write(jobName + "," + stageAndEdgeNumber._1 + "," + stageAndEdgeNumber._2);
                writer.newLine();
            }

        }

        writer.flush();
        writer.close();
        resultWriter.close();

        log.info("Stage number: {}, edge number: {}, job number: {}.", totalStages, totalEdges, totalJobs);
        log.info("Average stage number: {}, average edge number: {}", (totalStages / totalJobs), (totalEdges / totalJobs));

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
