package org.apache.spark.examples.aliTrace.MultistageEvaluation.scheduler;

/**
 * @author yonghui
 * @since 2020-10-02
 */
public interface Scheduler {
    public void submitResultStage(String stageId);
}
