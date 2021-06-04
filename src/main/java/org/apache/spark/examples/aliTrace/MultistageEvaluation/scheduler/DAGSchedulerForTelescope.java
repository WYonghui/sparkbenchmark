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
 * @since 2020-10-02
 */
public class DAGSchedulerForTelescope implements Scheduler{
    private static final Logger logger = LoggerFactory.getLogger(DAGSchedulerForTelescope.class);

    private Map<String, ArrayList<String>> stageDependencies;
    private Map<String, ArrayList<String>> childStages;
    private Set<String> finishedStages;
    private List<String> readyStages;
    private Map<String, List<Task>> taskSetsByStageId;
    private SortedList<String, Integer> stageForStartingTime;         // 阶段中任务可以开始执行的时间,从小到大排序
//    private Map<String, Double> readyStagePriorityList;        // 阶段按优先级从低到高排序的等待队列
    private Map<String, Double> stagePriorities;                      // 所有阶段的优先级
    private SortedTaskSlots taskSlots;
    private Integer parallelism;
    private Double weight;
    private Random random;

    public DAGSchedulerForTelescope(Map<String, ArrayList<String>> stageInfo, Integer coreNum, Integer parallelism, Double weight) {
        this.stageDependencies = stageInfo;
        this.finishedStages = new HashSet<>();
        this.readyStages = new ArrayList<>();
        this.taskSetsByStageId = new HashMap<>();
        this.stageForStartingTime = new SortedList<>();
//        this.readyStagePriorityList = new HashMap<>();
        this.stagePriorities = new HashMap<>();
        this.random = new Random(516);

        this.taskSlots = new SortedTaskSlots(coreNum);
        this.parallelism = parallelism;
        this.weight = weight;
    }

    public void findChildStages(String resultStage) {
        childStages = new HashMap<>();

        // 计算childStages
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.addLast(resultStage);
        while (!queue.isEmpty()) {
            String stage = queue.removeFirst();
            if (visited.contains(stage)) {
                continue;
            }

            List<String> parents = stageDependencies.get(stage);
            if (parents != null) {
                for (String parent: parents) {
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
     * 计算stage的后代阶段数量，作为stage的优先级
     * @param stage 目标stage
     * @return stage的后代阶段数量
     */
    private int getDescendantNumber(String stage) {
        int childNum = 0;
        Set<String> visited = new HashSet<>();

        // bfs查找所有的子阶段
        Deque<String> queue = new ArrayDeque<>();
        queue.add(stage);
        while (!queue.isEmpty()) {
            String cur = queue.removeFirst();
            List<String> children = childStages.get(cur);
            if (children == null) {
                continue;
            }
            for (String child: children) {
                if (!visited.contains(child)) {
                    visited.add(cur);
                    childNum++;
                    queue.addLast(child);
                }
            }
        }

        return childNum;
    }

    /**
     * 根据影响力计算公式计算每个阶段的影响力
     * 影响力计算公式为深度与子阶段数量的加权平均加上子阶段的最大影响力
     * @param resultStage result stage
     * @return stage的影响力
     */
    private void getStageInfluence(String resultStage) {

        stagePriorities.put(resultStage, 2.0); // result stage的influence设定为1
        Map<String, Integer> stageDepth = new HashMap<>();
        stageDepth.put(resultStage, 1);

        // bfd设定所有阶段的influence
        Deque<String> waitingStages = new LinkedList<>();
        waitingStages.add(resultStage);
        while (!waitingStages.isEmpty()) {
            String stage = waitingStages.removeFirst();
            List<String> parents = stageDependencies.get(stage);
            if (parents != null) {
                for (String parent: parents) {
                    if (stagePriorities.containsKey(parent)) {
                        continue;
                    }

                    List<String> children = childStages.get(parent);
                    double childrenMaxInfluence = 0;
                    int depth = 0;
                    boolean isChildrenInfluenceAvailable = true;
                    for (String child: children) {
                        if (!stagePriorities.containsKey(child)) { //如果某个子阶段还未计算出influence，则当前阶段不计算influence
                            isChildrenInfluenceAvailable = false;
                            break;
                        } else {
                            childrenMaxInfluence = Math.max(childrenMaxInfluence, stagePriorities.get(child));
                            depth = Math.max(depth, stageDepth.get(child));
                        }
                    }
                    if (isChildrenInfluenceAvailable) {
                        stageDepth.put(parent, ++depth);
                        double influence = weight * depth + (1 - weight) * children.size() + childrenMaxInfluence;
                        stagePriorities.put(parent, influence);
                        waitingStages.addLast(parent);
                    }

                }
            }

        }

    }

    /**
     * 根据影响力计算公式计算每个阶段的影响力
     * 影响力计算公式为深度与子阶段数量的加权平均
     * @param resultStage result stage
     * @return stage的影响力
     */
    private void getStageInfluence2(String resultStage) {

        stagePriorities.put(resultStage, 1.0); // result stage的influence设定为1
        Map<String, Integer> stageDepth = new HashMap<>();
        stageDepth.put(resultStage, 1);

        // bfs设定所有阶段的influences
        Deque<String> waitingStages = new LinkedList<>();
        waitingStages.add(resultStage);
        while (!waitingStages.isEmpty()) {
            String stage = waitingStages.removeFirst();
            List<String> parents = stageDependencies.get(stage);
            if (parents != null) {
                for (String parent: parents) {
                    if (stagePriorities.containsKey(parent)) {
                        continue;
                    }

                    List<String> children = childStages.get(parent);
                    Integer depth = 0;
                    boolean isChildrenInfluenceAvailable = true;
                    for (String child: children) {
                        // 如果某个子阶段还未计算出influence，则当前阶段不计算influence
                        // 可保证计算每个阶段时，获取到的深度是最大深度
                        if (!stagePriorities.containsKey(child)) {
                            isChildrenInfluenceAvailable = false;
                            break;
                        } else {
                            depth = Math.max(depth, stageDepth.get(child));
                        }
                    }

                    //所有子阶段都已经计算出influence，计算当前阶段的影响力
                    if (isChildrenInfluenceAvailable) {
                        stageDepth.put(parent, ++depth);
                        double influence = weight * depth + (1 - weight) * children.size();
                        stagePriorities.put(parent, influence);
                        waitingStages.addLast(parent);
                    }

                }
            }

        }

    }

    /**
     * 找出未提交的父阶段
     * @param stage
     * @return
     */
    public List<String> getMissingParentStages(String stage) {
        List<String> unfinishedParentStage = new ArrayList<>();

        List<String> parents = stageDependencies.get(stage);
        if (parents != null) {
            for (String parent: stageDependencies.get(stage)) {
                if (!isFinishedStage(parent)) {
                    unfinishedParentStage.add(parent);
                }
            }
        }

        return unfinishedParentStage;
    }


    private void submitStage(String stageId) {
        // dfs将所有的source stage加入到可调度队列中
        if (!isFinishedStage(stageId) && !isReadyStage(stageId)) {
            List<String> missingParents = getMissingParentStages(stageId);
            if (missingParents.isEmpty()) {
                readyStages.add(stageId);
//                // 根据子阶段数量为每个阶段设定优先级
//                int childNum = getDescendantNumber(stageId);
//                readyStagePriorityList.add(stageId, childNum + 1);

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
                    submitStage(parent);
                }
            }
        }
    }


    /**
     * 产生符合泊松分布的随机数
     * @param lamda
     * @return
     */
    private int getPossionVariable(double lamda) {
        int x = 0;
//        double y = Math.random();
        double y = random.nextDouble();
        double cdf = getPossionProbability(x, lamda);
        while (cdf < y) {
            x++;
            cdf += getPossionProbability(x, lamda);
        }
        return x;
    }

    private double getPossionProbability(int k, double lamda) {
        double c = Math.exp(-lamda), sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= lamda / i;
        }
        return sum * c;
    }

    public int submitResultStage(String stageId) {

        // 计算childStages
        findChildStages(stageId);

        // 计算影响力
        getStageInfluence2(stageId);

        // 将所有的source stage添加到ready stage列表中
        submitStage(stageId);

        // 从就绪队列中调度可调度的阶段
        // 按阶段开始时间的顺序调度，如果多个阶段开始时间相同，按优先级的顺序调度
        while (!stageForStartingTime.isEmpty()) {
//            SortedList.Entry<String, Double> first = readyStagePriorityList.removeLast();
//            String readyStage = first.getId();
//            double priority = first.getValue();
//            Integer startTime = stageForStartingTime.remove(readyStage);
            // 选出队列中第一个被调度的程序
            // 挑选规则是优先选开始时间最早的阶段。如果有多个阶段的开始时间相同，则选择优先级最高的阶段
//            List<String> stageWithSameStartTime = new ArrayList<>();
            SortedList<String, Double> stageWithSameStartTime = new SortedList<>();
            Iterator<SortedList.Entry<String, Integer>> iterator = stageForStartingTime.iterator();
            SortedList.Entry<String, Integer> firstEntry = iterator.next();
            Integer startTime = firstEntry.getValue();
            stageWithSameStartTime.add(firstEntry.getId(), stagePriorities.get(firstEntry.getId()));
            while (iterator.hasNext()) {
                SortedList.Entry<String, Integer> entry = iterator.next();
                if (entry.getValue().equals(startTime)) {
                    stageWithSameStartTime.add(entry.getId(), stagePriorities.get(entry.getId()));
                } else {
                    break;
                }
            }

            // 按照规则，找出第一个被调度的阶段
            SortedList.Entry<String, Double> entry = stageWithSameStartTime.removeLast();
            String readyStage = entry.getId();
            double priority = entry.getValue();
//            String readyStage = firstEntry.getId();
//            double priority = stagePriorities.get(readyStage);
//            for (String stage: stageWithSameStartTime) {
//                if (stagePriorities.get(stage) > priority) {
//                    readyStage = stage;
//                    priority = stagePriorities.get(stage);
//                }
//            }
            stageForStartingTime.removeByKey(readyStage);


            // 执行该stage
            logger.debug("Runs stage {}, priority is {}.", readyStage, priority);
            List<Task> taskSet = new ArrayList<>(parallelism);
//            int taskDuration = getPossionVariable(8);
            int taskDuration = random.nextInt(50) + 5;
            for (int i = 0; i < parallelism; i++) {
                // 计算该task的完成时间，更新taskSlot完成该task的时间
                TaskSlot slot = taskSlots.getFirst();
                Task task = new Task(priority, taskDuration);
                task.finishTime = Math.max(slot.getAvailableTime(), startTime) + task.duration;
                slot.setAvailableTime(task.finishTime);
                taskSlots.add(slot);

                taskSet.add(task);
            }
            taskSetsByStageId.put(readyStage, taskSet);

            // 执行完成后，将该stage从ready stage队列中取出，放入finished stage队列
            readyStages.remove(readyStage);
            finishedStages.add(readyStage);

            // 判断子阶段能够被提交，如果可以，将子阶段加入到ready stage队列中
            List<String> children = childStages.get(readyStage);
            if (children != null) {
                for (String child: children) {
                    submitStage(child);
                }
            }

        }

        int jct = taskSlots.getLast().getAvailableTime();
        logger.debug("Job completion time is {}s in Telescope", jct);
        return jct;

    }

    private boolean isFinishedStage(String stageId) {
        return finishedStages.contains(stageId);
    }

    private boolean isReadyStage(String stageId) {
        return readyStages.contains(stageId);
    }
}
