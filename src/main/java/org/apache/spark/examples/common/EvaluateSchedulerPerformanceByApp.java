package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 *
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.EvaluateSchedulerPerformanceByApp \
     jobInfo.csv schedulerPerformance.csv
 */
public class EvaluateSchedulerPerformanceByApp {
    private static Logger LOG = LoggerFactory.getLogger(EvaluateSchedulerPerformanceByApp.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.info("Usage: EvaluateSchedulerPerformance <jobInfo> <output.csv>");
            System.exit(-1);
        }

        String input = args[0];
        String result = args[1];
        FileInputStream inputStream = new FileInputStream(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        HashMap<Integer, Integer> stageNumberAndEvaluateTimes = new LinkedHashMap<>();
        HashMap<Integer, List<Long>> stageNumberAndExecutionTime = new LinkedHashMap<>();
        EvaluateSchedulerPerformanceByApp schedulerPerformance = new EvaluateSchedulerPerformanceByApp();

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

            // 分析出result stage
            Set<String> resultStages = new HashSet<>();
            Set<String> allStages = new HashSet<>();
            boolean notDAG = false;
            for (String resultStage : maybeResultStages) {

                if (notResultStages.contains(resultStage)) {
                    continue;
                }

//                计算该app中包含的阶段数量
//                分析每一个resultStage构成的job，重复使用的stage仅计算一次
                Queue<String> waitingForVisit = new LinkedList<>();
                waitingForVisit.offer(resultStage);
                int flag = 0;
                while (!waitingForVisit.isEmpty()) {
                    String stage = waitingForVisit.poll();
                    allStages.add(stage);

                    Set<String> fathers = stageToFathers.get(stage);
                    if (fathers != null) {
                        for (String father : fathers) {
                            waitingForVisit.offer(father);
                        }
                    }
                    flag++;    // 如果该job存在环路，flag将不断增加。当flag大于100时，认为该job存在环路，是非DAG型作业
                    if (flag > 100) {
                        notDAG = true;
                        break;
                    }
                }

                if (notDAG) {
                    break;
                }

//                将resultStage添加到result Stage集合中
                resultStages.add(resultStage);

            }

            if (notDAG) {
                continue;
            }

            Integer stagesNum = allStages.size();

//            如果该数量已经测试过且测试次数大于5次，则直接跳过
            if (stageNumberAndEvaluateTimes.containsKey(stagesNum)) {
                if (stageNumberAndEvaluateTimes.get(stagesNum) > 5) {
                    continue;
                }
            }

//            未测试过的阶段数，为其生成测试次数和测试结果list
            if (!stageNumberAndEvaluateTimes.containsKey(stagesNum)) {
                stageNumberAndEvaluateTimes.put(stagesNum, 0);
                stageNumberAndExecutionTime.put(stagesNum, new ArrayList<Long>());
            }
            Integer evaluationTimes = stageNumberAndEvaluateTimes.get(stagesNum);
            stageNumberAndEvaluateTimes.put(stagesNum, evaluationTimes+1); // 更新测试次数
            List<Long> executionTimeList = stageNumberAndExecutionTime.get(stagesNum);

            Long start = System.nanoTime();
//            分别分析每个resultStage构成的job
            for (String resultStage : resultStages) {

//                分析出该job内stage的依赖关系
                HashMap<String, Set<String>> stageDependencies = new HashMap<>();
                Queue<String> queue = new LinkedList<>();
                queue.offer(resultStage);
                while (!queue.isEmpty()) {
                    String stage = queue.poll();

                    Set<String> fathers = stageToFathers.get(stage);
                    if (fathers != null) {
                        stageDependencies.put(stage, fathers);
                        for (String father: stageToFathers.get(stage)) {
                            queue.offer(father);
                        }
                    } else {
                        stageDependencies.put(stage, new HashSet<String>());
                    }

                }

//                进行测试
                schedulerPerformance.scheduler(resultStage, stageDependencies);

            }
            Long end = System.nanoTime();
            executionTimeList.add(end - start);
            stageNumberAndExecutionTime.put(stagesNum, executionTimeList);
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
            StringBuilder out = new StringBuilder();
            out.append(entry.getKey());
            for (Long time : entry.getValue()) {
                out.append(",");
                out.append(time);
            }
            writer.write(out.toString());
            writer.newLine();
        }

        reader.close();
        inputStream.close();
        writer.flush();
        writer.close();
        outputStream.close();

    }

    /**
     * 根据resultStage和依赖关系确定当前作业的调度顺序
     * @param resultStage
     * @param stageDependencies
     */
    private void scheduler(String resultStage, HashMap<String, Set<String>> stageDependencies) {

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
