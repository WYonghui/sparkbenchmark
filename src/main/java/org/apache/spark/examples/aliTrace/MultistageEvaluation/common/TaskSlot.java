package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

/**
 * 计算单元，包含该计算单元可用的时间
 */
public class TaskSlot {
    private Integer id;
    private Integer availableTime;

    /**
     * Instantiates a new Task slot.
     *
     * @param id            the id
     * @param availableTime the available time
     */
    public TaskSlot(Integer id, Integer availableTime) {
        this.id = id;
        this.availableTime = availableTime;
    }

    public TaskSlot(Integer id) {
        this.id = id;
        this.availableTime = 0;
    }

    public Integer getId() {
        return id;
    }

    public Integer getAvailableTime() {
        return availableTime;
    }

    public void setAvailableTime(Integer availableTime) {
        this.availableTime = availableTime;
    }
}
