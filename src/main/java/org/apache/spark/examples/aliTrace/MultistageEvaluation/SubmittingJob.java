package org.apache.spark.examples.aliTrace.MultistageEvaluation;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.JobCommandParser;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.DAGScheduler;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.DAGSchedulerForTelescope;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Submitting a job to the cluster, the scheduler proposes a scheduling plan and calculates the job completion time.
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.aliTrace.MultistageEvaluation.SubmittingJob \
     -j F:\telescope\测试\阿里巴巴数据集\指定作业的信息\j_3731856-30.csv -c 480 -p 600 -s telescope -w 0.2
 * @author yonghui
 * @date 2020-09-29
 */
public class SubmittingJob {
    private static final Logger logger = LoggerFactory.getLogger(SubmittingJob.class);

    Map<String, ArrayList<String>> stageInfo; // 存放每个阶段的父阶段
    String resultStage; // 拥有最多阶段数的job的result stage
    Scheduler dagScheduler;

    public SubmittingJob(String clusterType, Integer coreNum, Integer parallelism, Double weight) {
        this.stageInfo = new HashMap<>();
        switch (clusterType.toLowerCase()) {
            case "spark":
                this.dagScheduler = new DAGScheduler(stageInfo, coreNum, parallelism);
                break;
            case "telescope":
                this.dagScheduler = new DAGSchedulerForTelescope(stageInfo, coreNum, parallelism, weight);
                break;
        }

    }

    public int submitJob() {
        return dagScheduler.submitResultStage(resultStage);
    }

    /**
     * 分析作业拓扑，找出result stage和作业中阶段之间的依赖关系
     * @param path 提交作业的路径
     */
    public void init(String path) {
        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader reader = new BufferedReader(fileReader);

            Set<String> allStages = new HashSet<>();
            Set<String> nonleafStages = new HashSet<>();
            String taskName = null;
            while ((taskName = reader.readLine()) != null) {
                if (taskName.equals("")) continue;
                String[] strs = taskName.split("_");
                String curStage = strs[0].substring(1);

                allStages.add(curStage);
                ArrayList<String> parentStages = new ArrayList<>();
                for (int i = 1; i < strs.length; i++) {
                    parentStages.add(strs[i]);
                    nonleafStages.add(strs[i]);
                }
                stageInfo.put(curStage, parentStages);
            }

            // 关闭文件
            reader.close();
            fileReader.close();

            // 找出作业中stage数量最多的作业的result stage
            int maxStages = 0;
            String maxResultStage = null;
            Iterator<String> iterator = allStages.iterator();
            while (iterator.hasNext()) {
                String resultStage = iterator.next();
                if (!nonleafStages.contains(resultStage)) {
                    // bfs找stage数量
                    int stageNum = 0;
                    Deque<String> queue = new ArrayDeque<>();
                    Map<String, Boolean> visited = new HashMap<>();

                    queue.add(resultStage);
                    while (!queue.isEmpty()) {
                        String stage = queue.removeFirst();
                        stageNum++;
                        visited.put(stage, true);

                        List<String> parents = stageInfo.get(stage);
                        if (parents != null) {
                            for (String parent: parents) {
                                if (!visited.containsKey(parent)) {
                                    queue.addLast(parent);
                                }
                            }
                        }
                    }

                    if (stageNum > maxStages) {
                        maxStages = stageNum;
                        maxResultStage = resultStage;
                    }
                }
            }

            this.resultStage = maxResultStage;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        JobCommandParser parser = new JobCommandParser("SubmittingJob");
        CommandLine commandLine = parser.parse(args);

        String jobPath = commandLine.getOptionValue('j');
        String clusterType = commandLine.getOptionValue('s');
        Integer coreNum = Integer.valueOf(commandLine.getOptionValue('c'));
        Integer parallelism = Integer.valueOf(commandLine.getOptionValue('p'));
        Double weight = 0.5;
        if (commandLine.hasOption('w'))
            weight = Double.valueOf(commandLine.getOptionValue('w'));

        logger.info("SubmittingJob -j {} \\", jobPath);
        logger.info("-s {} -c {} -p {}", clusterType, coreNum, parallelism);
        SubmittingJob submit = new SubmittingJob(clusterType, coreNum, parallelism, weight);
        submit.init(jobPath);
        submit.submitJob();

    }
}
