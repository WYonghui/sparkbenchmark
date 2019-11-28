package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;


/**
 * The type Max schedulable stages.
 */
/*
* 分析alibaba/clusterdata/cluster-trace-v2018/batch_task.csv中DAG型job的最大可调度阶段数
* 说明：其中包含一些job存在环路，这种情况排除在外。还有一些task没有信息，但是被其他task依赖，这种task也在计算之中
* java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.MaxSchedulableStages \
    E:\download\ali-cluster-Data\batch_task.csv E:\download\ali-cluster-Data\batch_task_nocycle.csv
 */
public class MaxSchedulableStages {

    /**
     * The constant log.
     */
    final static Logger log = LoggerFactory.getLogger(MaxSchedulableStages.class);

    private boolean check(HashMap<String, Set<String>> taskInfo, String curTask, String curParentTask) {
        Set<String> parentTasks = taskInfo.get(curTask);

        Iterator<String> iterator = parentTasks.iterator();  //遍历所有父节点
        while (iterator.hasNext()) {
            String parentTask = iterator.next();
            if (parentTask == curParentTask) {
                continue;
            }

            if (taskInfo.get(parentTask) == null) {
                return false;
            } else if (taskInfo.get(parentTask).contains(curParentTask)) {
                return true;
            } else if (taskInfo.get(parentTask).size() == 0){
                return false;
            } else {
                return check(taskInfo, parentTask, curParentTask);
            }
        }

        //无其他父节点
        return false;
    }

    //为每一个阶段赋予一个层值，最大层值就是最大可调度阶段数
    private Integer stagesMark(HashMap<String, Set<String>> taskInfo, String curTask, HashMap<String, Integer> rating) {
        Set<String> parentTasks = taskInfo.get(curTask);
        Integer rank = rating.get(curTask); //当前task的rank值
        Integer maxRank = rank; //初始时，第一个parent的rank值等于当前rank值

        //递归出口，某些节点不曾单独出现
        if (parentTasks == null) {
            return rank;
        }

        //递归出口是没有父节点的数据输入节点
        if (parentTasks.size() == 0) {
            return rank;
        }

        //为每一个父节点设定层级
        Iterator<String> iterator = parentTasks.iterator();

        while (iterator.hasNext()) {
            String parentTask = iterator.next();

            //如果已经赋过值，不必重复赋值
            if (rating.containsKey(parentTask)) {
                continue;
            }

            //如果存在环路，则不是DAG
            if (check(taskInfo, curTask, curTask)) {
                rating.put("-1", -1); //代表此Job中存在环路
                return -1;
            }

            //给一个task赋值之前，先判断它不会被其他的父task直接或间接的依赖
            //如果被依赖，则跳过对此父task赋值
            if (check(taskInfo, curTask, parentTask)) {
                continue;
            }


            rating.put(parentTask, maxRank);
            maxRank = stagesMark(taskInfo, parentTask, rating);
            maxRank++;

        }

        return (maxRank - 1);

    }

    private Integer schedulableStages(ArrayList<String> tasks) {
        //key是当前task，value是父task集合
        HashMap<String, Set<String>> taskInfo = new HashMap<>();
        Iterator<String> iterator = tasks.iterator();

        Set<String> allTasks = new HashSet<>();
        Set<String> middleTasks = new HashSet<>();


        //对task逐个分析，拆分出当前task及其依赖的父task
        while (iterator.hasNext()) {
            Set<String> parentTasks = new HashSet<>();
            String taskName = iterator.next();
            String[] strs = taskName.split("_");
            String curTask = strs[0].substring(1);

            allTasks.add(curTask);
            for (int i = 1; i < strs.length; i++) {
                middleTasks.add(strs[i]);
                allTasks.add(strs[i]);
                parentTasks.add(strs[i]);
            }
            taskInfo.put(curTask, parentTasks);

        }

        //只有1个stage的job，是非DAG型job
        if (allTasks.size() < 2) {
            return -1;
        }

        //分析出result stage
        Iterator<String> allTasksIterator = allTasks.iterator();
        Integer maxRanks = 0;
        while (allTasksIterator.hasNext()) {
            String resultTask = allTasksIterator.next();
            if (middleTasks.contains(resultTask)) {
                continue; //排除中间task
            }

            //分析最多可调度阶段数
            HashMap<String, Integer> rating = new HashMap<>();
            rating.put(resultTask, 1);

            Integer maxRank = stagesMark(taskInfo, resultTask, rating);
            if (rating.containsKey("-1")) {
                if (rating.get("-1").equals(-1)) {
                    return -1;
                }
            }

            if (maxRank > maxRanks) {
                maxRanks = maxRank;
            }

        }

        return maxRanks;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            log.warn("Usage: java <mainClass> <srcFile> <resultFile>");
            System.exit(-1);
        }

        String srcFile = args[0];
        String result = args[1];
        HashMap<String, ArrayList<String>> jobInfo = new HashMap<>();
        MaxSchedulableStages schedulableStages = new MaxSchedulableStages();

        //读取输入文件
        FileInputStream inputStream = new FileInputStream(srcFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        //提取job信息和task信息
        String record = null;
        while ((record = reader.readLine()) != null) {
            Pattern COMMA = Pattern.compile(",");
            String[] attrs = COMMA.split(record);
            String jobName = attrs[2];
            String taskInfo = attrs[0];

            //跳过tasks that are not DAGs
            if (taskInfo.contains("task_") || taskInfo.contains("MergeTask")) {
                continue;
            }

            ArrayList<String> tasks = jobInfo.get(jobName);
            if (tasks == null) {
                tasks = new ArrayList<>();
                jobInfo.put(jobName, tasks);
            }

            tasks.add(taskInfo);

        }
        log.info("按Job分离tasks完成！");
        reader.close();
        inputStream.close();

        //开始分析每个Job的最大可调度阶段数
        FileOutputStream outputStream = new FileOutputStream(result);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

        //逐个Job分析
        Set<String> jobs = jobInfo.keySet();
        Iterator<String> jobIterator = jobs.iterator();
        while (jobIterator.hasNext()) {
            String jobName = jobIterator.next();
            ArrayList<String> tasks = jobInfo.get(jobName);

//            if (!jobName.equals("j_319918")) {
//                continue;
//            }

            //计算最多可调度阶段数
            Integer maxStages = schedulableStages.schedulableStages(tasks);

            //去掉非DAG型作业
            if (maxStages == -1) {
                continue;
            }

            writer.write(jobName + "," + maxStages);
            writer.newLine();
        }

        writer.flush();
        writer.close();
        outputStream.close();

    }
}
