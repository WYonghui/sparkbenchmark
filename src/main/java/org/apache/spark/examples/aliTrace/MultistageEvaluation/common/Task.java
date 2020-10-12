package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

/**
 * @Author: yonghui
 * @Date: 2020-09-29
 * @Description:
 */
public class Task {
    int priority;
    public int duration;
    public int finishTime;

    public Task() {
        this.priority = 1;
        this.duration = 1;
    }

    public Task(int duration) {
        this.priority = 1;
        this.duration = duration;
    }

    public Task(int priority, int duration) {
        this.priority = priority;
        this.duration = duration;
    }
}
