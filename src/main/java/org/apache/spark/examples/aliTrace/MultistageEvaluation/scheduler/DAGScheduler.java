package org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler;

import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.SortedList;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.SortedTaskSlots;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.Task;
import org.apache.spark.examples.aliTrace.MultistageEvaluation.common.TaskSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yonghui
 * @since 2020-09-30
 */
public class DAGScheduler implements Scheduler{
    private static final Logger logger = LoggerFactory.getLogger(DAGScheduler.class);

    Map<String, ArrayList<String>> stageDependencies;
    Map<String, ArrayList<String>> childStages;
    Set<String> finishedStages;
    List<String> readyStages;
    Map<String, List<Task>> taskSetsByStageId;
    SortedList stageForStartingTime;
    SortedTaskSlots taskSlots;
    Integer parallelism;


    public DAGScheduler(Map<String, ArrayList<String>> stageInfo, Integer coreNum, Integer parallelism) {
        this.stageDependencies = stageInfo;
        this.finishedStages = new HashSet<>();
        this.readyStages = new ArrayList<>();
        this.taskSetsByStageId = new HashMap<>();
        this.stageForStartingTime = new SortedList();

        this.taskSlots = new SortedTaskSlots(coreNum);
        this.parallelism = parallelism;
    }

    public void findChildStages(String resultStage) {
        childStages = new HashMap<>();

        // 计算childStages
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.addLast(resultStage);
        while (!queue.isEmpty()) {
            String stage = queue.removeFirst();
            List<String> parents = stageDependencies.get(stage);
//            if (parents != null)
            for (String parent: parents) {
                if (!visited.contains(parent)) {
                    ArrayList<String> children = childStages.get(parent);
                    if (children == null) {
                        children = new ArrayList<>();
                    }
                    children.add(stage);
                    childStages.put(parent, children);

                    queue.add(parent);
                }

            }

            visited.add(stage);
        }
    }

    /**
     * 找出未提交的父阶段
     * @param stage
     * @return
     */
    public List<String> getMissingParentStages(String stage) {
        List<String> unfinishedParentStage = new ArrayList<>();
        for (String parent: stageDependencies.get(stage)) {
            if (!isFinishedStage(parent)) {
                unfinishedParentStage.add(parent);
            }
        }

        return unfinishedParentStage;
    }


    public void submitStage(String stageId) {
        // dfs将所有的source stage加入到可调度队列中
        if (!isFinishedStage(stageId) && !isReadyStage(stageId)) {
            List<String> missingParents = getMissingParentStages(stageId);
            if (missingParents.isEmpty()) {
                readyStages.add(stageId);

                // 计算当前stage中任务的最早开始时间
                // 分析各个父阶段中任务的最晚完成时间，作为当前阶段中任务的最早开始时间
                List<String> parents = stageDependencies.get(stageId);
                if (parents == null || parents.isEmpty()) {
                    stageForStartingTime.add(stageId, 0);
                } else {
                    int latestTime = 0;
                    for (String parent: parents) {
                        List<Task> taskSet = taskSetsByStageId.get(parent);
                        for (Task task: taskSet) {
                            latestTime = Math.max(latestTime, task.finishTime);
                        }
                    }
                    stageForStartingTime.add(stageId, latestTime);
                }

            } else {
                for (String parent: missingParents) {
//                    logger.info("Submits stage {}", parent);
                    submitStage(parent);
                }
            }
        }
    }


    public void submitResultStage(String stageId) {
        // 计算childStages
        findChildStages(stageId);

        // 将所有的source stage添加到ready stage列表中
        submitStage(stageId);

        // 从就绪队列中调度可调度的阶段
        // 按阶段开始时间的顺序调度
        while (!stageForStartingTime.isEmpty()) {
            SortedList.Entry<String, Integer> first = stageForStartingTime.removeFirst();
            String readyStage = first.getId();
            Integer startTime = first.getValue();
            // 执行该stage
            logger.debug("Runs stage {}", readyStage);
            List<Task> taskSet = new ArrayList<>(parallelism);
            for (int i = 0; i < parallelism; i++) {
                // 计算该task的完成时间，更新taskSlot完成该task的时间
                TaskSlot slot = taskSlots.getFirst();
                Task task = new Task(1);
                task.finishTime = Math.max(slot.getAvailableTime(), startTime) + task.duration;
                slot.setAvailableTime(task.finishTime);
                taskSlots.add(slot);

                taskSet.add(task);
            }
            taskSetsByStageId.put(readyStage, taskSet);

            // 执行完成后，将该stage从ready stage队列中取出，放入finished stage队列
            readyStages.remove(readyStage);
//            stageForStartingTime.removeFirst();
            finishedStages.add(readyStage);

            // 判断子阶段能够被提交，如果可以，将子阶段加入到ready stage队列中
            List<String> children = childStages.get(readyStage);
            if (children != null) {
                for (String child: children) {
                    submitStage(child);
                }
            }

        }

        logger.info("Job completion time is {}s in Spark", taskSlots.getLast().getAvailableTime());

    }

    public boolean isFinishedStage(String stageId) {
        return finishedStages.contains(stageId);
    }

    public boolean isReadyStage(String stageId) {
        return readyStages.contains(stageId);
    }

}
