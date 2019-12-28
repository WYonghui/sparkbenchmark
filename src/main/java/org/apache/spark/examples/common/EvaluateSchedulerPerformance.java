package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.EvaluateSchedulerPerformance \
       jobInfo.csv schedulerPerformance.csv
 */
public class EvaluateSchedulerPerformance {
    private static Logger LOG = LoggerFactory.getLogger(EvaluateSchedulerPerformance.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.info("Usage: EvaluateSchedulerPerformance <jobInfo> <schedulerPerformance>");
            System.exit(-1);
        }

        String input = args[0];
        String result = args[1];
        FileInputStream inputStream = new FileInputStream(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        HashMap<Integer, Integer> stageNumberAndEvaluateTimes = new LinkedHashMap<>();
        HashMap<Integer, List<Long>> stageNumberAndExecutionTime = new LinkedHashMap<>();
        EvaluateSchedulerPerformance schedulerPerformance = new EvaluateSchedulerPerformance();

        //逐个分析job
        String jobInfo;
        while ((jobInfo = reader.readLine()) != null) {
            //提取出jobId和stage及依赖关系
            String[] items = jobInfo.split(",\\[|, |\\]");
//            String jobId = items[0];

            //跳过只有1个stage的job
            if (items.length <= 2) {
                continue;
            }

            HashMap<String, Set<String>> stageToFathers = new HashMap<>();
            Set<String> maybeResultStages = new HashSet<>(); // 每一个stage都可能是result stage
            Set<String> notResultStages = new HashSet<>(); // 被其他阶段依赖的stage一定不是result stage
            // 分析每个stage的依赖
            for (int i = 1; i < items.length; i++) {
                String[] stages = items[i].split("_");

                // 分析出每个stage的父依赖，添加到hash表中
                Set<String> fathers = new HashSet<>();
                for (int j = 1; j < stages.length; j++) {
                    fathers.add(stages[j]);
                    notResultStages.add(stages[j]);
                }
                stageToFathers.put(stages[0].substring(1), fathers);
                maybeResultStages.add(stages[0].substring(1));
            }

            // 分析每个result stage，找出job中stage数最多的作业
            Iterator<String> iterator = maybeResultStages.iterator();
            while (iterator.hasNext()) {
                String resultStage = iterator.next();
                if (notResultStages.contains(resultStage)) {
                    continue;
                }

                // 分析出以该result stage构成的作业包含的阶段数量及阶段之间的依赖关系
                Integer stagesNum = 0;
                HashMap<String, Set<String>> stageDependencies = new HashMap<>();
                Queue<String> queue = new LinkedList<>();
                queue.offer(resultStage);
                while (!queue.isEmpty()) {
                    String stage = queue.poll();
                    stagesNum += 1;
                    Set<String> fathers = stageDependencies.put(stage, stageToFathers.get(stage));
                    if (fathers != null) { //存在死循环，此作业非DAG
                        stagesNum = -1;
                        break;
                    }

                    Set<String> fathers2 = stageToFathers.get(stage);
                    if (fathers2 != null) {
                        for (String father: stageToFathers.get(stage)) {
                            queue.offer(father);
                        }
                    }
                }

                if (stagesNum < 0) continue;

//                 未测试过的阶段数，进行测试
                if (!stageNumberAndEvaluateTimes.containsKey(stagesNum)) {
                    stageNumberAndEvaluateTimes.put(stagesNum, 1);
                    List<Long> executionTimeList = new ArrayList<>();

//                    进行测试
                    Long executionTime = schedulerPerformance.scheduler(resultStage, stageDependencies);

//                    将测试结果添加到hash表中
                    executionTimeList.add(executionTime);
                    stageNumberAndExecutionTime.put(stagesNum, executionTimeList);

                } else if (stageNumberAndEvaluateTimes.get(stagesNum) < 5) {
//                  测试次数少于5次的阶段数，也可以进行测试
                    Integer evaluationTimes = stageNumberAndEvaluateTimes.get(stagesNum);
                    stageNumberAndEvaluateTimes.put(stagesNum, evaluationTimes+1); // 更新测试次数

                    Long executionTime = schedulerPerformance.scheduler(resultStage, stageDependencies);
                    List<Long> executionTimeList = stageNumberAndExecutionTime.get(stagesNum);
                    executionTimeList.add(executionTime);
                    stageNumberAndExecutionTime.put(stagesNum, executionTimeList);

                }
//                测试过且测试次数大于5次的阶段数，直接跳过

            }

        }

//        对测试结果按key进行排序
        ArrayList<Map.Entry<Integer, List<Long>>> list = new ArrayList<>(stageNumberAndExecutionTime.entrySet());
        list.sort( new Comparator<Map.Entry<Integer, List<Long>>>() {
            @Override
            public int compare(Map.Entry<Integer, List<Long>> o1, Map.Entry<Integer, List<Long>> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        FileOutputStream outputStream = new FileOutputStream(result);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

        for (Map.Entry<Integer, List<Long>> entry : list) {
            String out = "" + entry.getKey();
            for (Long time : entry.getValue()) {
                out = out + "," + time;
            }
            writer.write(out);
            writer.newLine();
        }

        reader.close();
        inputStream.close();
        writer.flush();
        writer.close();
        outputStream.close();

    }

    private Long scheduler(String resultStage, HashMap<String, Set<String>> stageDependencies) {

        Long start = System.nanoTime();
//        分析出每个阶段的子阶段
        HashMap<String, HashSet<String>> stageToChildren = new HashMap<>();
        resolveChildren(resultStage, stageDependencies, stageToChildren);

        HashMap<String, Integer> stagesPriority = new HashMap<>();
        stagesPriority.put(resultStage, 1);

//        采用宽度优先的方式，自下而上进行遍历
        Queue<String> waitingForVisited = new LinkedList<>();
        waitingForVisited.offer(resultStage);

        while (!waitingForVisited.isEmpty()){
            String stage = waitingForVisited.poll();
            Set<String> fathers = stageDependencies.get(stage);

            for (String father : fathers) {
                HashSet<String> brothers = stageToChildren.get(father);
                Integer maxPriority = 0;

                for (String brother : brothers) {
                    if (stagesPriority.containsKey(brother)) {
                        Integer brotherPriority = stagesPriority.get(brother);
                        maxPriority = (maxPriority < brotherPriority) ? brotherPriority : maxPriority;

                    } else {
                        maxPriority = 0;
                        break;
                    }

                }

                if (maxPriority != 0) {
                    stagesPriority.put(father, maxPriority + brothers.size());
                    waitingForVisited.offer(father);
                }

            }

        }

//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        Long end = System.nanoTime();

        return (end - start);
    }

    /**
     * 利用阶段和父依赖信息，分析出每个阶段的子阶段
     * @param stage
     * @param stageDependencies
     * @param stageToChildren
     */
    private void resolveChildren(String stage,
                                 HashMap<String, Set<String>> stageDependencies,
                                 HashMap<String, HashSet<String>> stageToChildren) {

        Set<String> fathers = stageDependencies.get(stage);
        for (String father : fathers) {
            HashSet<String> brothers = stageToChildren.get(father);
            boolean flag = true;
            if (brothers == null) {
                brothers = new HashSet<>();
                flag = false;
            }
            brothers.add(stage);
            stageToChildren.put(father, brothers);

            if (flag == false) { // 如果father已经存在于stageToChildren中，说明father已经被分析过，无需重复分析
                resolveChildren(father, stageDependencies, stageToChildren);
            }
        }

    }
}

//    String str = "j_319975,[R9_1_4_8, R6_5, M5, M7_2_3_6, R8_7, M3, R10_9, R4_7, M2, R1_7]";
//    String[] strs = str.split(",\\[|, |\\]");
//        for (int i = 0; i < strs.length; i++) {
//        LOG.info(strs[i]);
//        }