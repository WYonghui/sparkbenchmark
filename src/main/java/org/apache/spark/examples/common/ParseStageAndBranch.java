package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class ParseStageAndBranch {
    private static Logger LOG = LoggerFactory.getLogger(ParseStageAndBranch.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.info("Usage: ParseStageAndBranch <jobInfo> <stageAndBranch.out>");
            System.exit(-1);
        }

        String input = args[0];
        String result = args[1];
        FileInputStream inputStream = new FileInputStream(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        HashMap<Integer, List<Integer>> stageNumberAndBranchNumber = new LinkedHashMap<>();

        // 逐个分析job
        String jobInfo;
        while ((jobInfo = reader.readLine()) != null) {
//            if (!jobInfo.equals("j_3949586,[M3, M5, M1, M2, M6, M8, M4, M7]")) {
//                continue;
//            }
            // 提取出jobId和stage及依赖关系
            String[] items = jobInfo.split(",\\[|, |\\]");

            // 跳过只有1个stage的job
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

            // 分析每个result stage，计算该ResultStage所在作业包含的stage数量和branch数量
            // 将计算结果累加到该应用的stage数量和branch数量上
            // 首先找出result stage
            Set<String> resultStages = new HashSet<>();
            Set<String> allStages = new HashSet<>();
            boolean notDAG = false;
            for (String resultStage : maybeResultStages) {
                if (notResultStages.contains(resultStage)) {
                    continue;
                }

                // 分析该应用包含的stage数量和branch数量
                // 分析每个result stage构成的作业，重复使用的stage仅计算一次，重复出现的branch仅计算一次
                Queue<String> waitingForVisit = new LinkedList<>();
                waitingForVisit.offer(resultStage);
                int flag = 0;
                while (!waitingForVisit.isEmpty()) {
                    String stage = waitingForVisit.poll();
                    if (allStages.contains(stage)) {
                        continue;
                    }
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
                // 将resultStage添加到result Stage集合中
                resultStages.add(resultStage);
            }

            if (notDAG) { // 如果存在环路，该应用不再计算
                continue;
            }
            int stageNum = allStages.size();

            // 找出了所有的ResultStage,根据ResultStage计算branch数量
            int branchNum = 0;
            Set<String> visitedStages = new HashSet<>();
            for (String resultStage: resultStages) {
                Queue<String> waitingForVisit = new LinkedList<>();

                Set<String> resultFathers = stageToFathers.get(resultStage);
                if (resultFathers != null) {
                    if (resultFathers.size() <= 1) { // resultStage只有一个或没有父阶段时，为该resultStage生成一个branch
                        branchNum += 1;
                    }
                } else {
                    branchNum += 1;
                }
                waitingForVisit.offer(resultStage);
                while (!waitingForVisit.isEmpty()) {
                    String stage = waitingForVisit.poll();
                    if (visitedStages.contains(stage)) {
                        continue;
                    }
                    visitedStages.add(stage);

                    Set<String> fathers = stageToFathers.get(stage);
                    if (fathers != null) {
                        if (fathers.size() > 1) {
                            branchNum += fathers.size();
                        }
                        for (String father : fathers) {
                            waitingForVisit.offer(father);
                        }
                    }

                }
            }

            if (branchNum == 0) {
                LOG.info(jobInfo);
            }

            List<Integer> list = null;
            // 如果阶段数之前未出现过，则为其创建branchNumber列表
            if (!stageNumberAndBranchNumber.containsKey(stageNum)) {
                list = new ArrayList<>();
            } else {
                list = stageNumberAndBranchNumber.get(stageNum);
            }
            list.add(branchNum);
            stageNumberAndBranchNumber.put(stageNum, list);
        }

        // 计算平均branch数量
        Iterator<Integer> iterator = stageNumberAndBranchNumber.keySet().iterator();
        while (iterator.hasNext()) {
            Integer stageNum = iterator.next();
            List<Integer> branchNums = stageNumberAndBranchNumber.get(stageNum);
            Integer sum = 0;
            for (Integer branchNum : branchNums) {
                sum += branchNum;
            }
            Integer avgBranchNum = (int)Math.ceil((double)sum / branchNums.size());
            List<Integer> list = new ArrayList<>();
            list.add(avgBranchNum);
            stageNumberAndBranchNumber.put(stageNum, list);
        }


        // 对测试结果按key(stage数量)进行排序
        ArrayList<Map.Entry<Integer, List<Integer>>> list = new ArrayList<>(stageNumberAndBranchNumber.entrySet());
        list.sort( new Comparator<Map.Entry<Integer, List<Integer>>>() {
            @Override
            public int compare(Map.Entry<Integer, List<Integer>> o1, Map.Entry<Integer, List<Integer>> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        FileOutputStream outputStream = new FileOutputStream(result);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

        for (Map.Entry<Integer, List<Integer>> entry : list) {
            StringBuilder out = new StringBuilder();
            out.append(entry.getKey());
            for (Integer branchNum : entry.getValue()) {
                out.append(",");
                out.append(branchNum);
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

}
