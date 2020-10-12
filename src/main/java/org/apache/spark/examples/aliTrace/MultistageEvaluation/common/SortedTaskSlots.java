package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 构造一个双向链表，用于存放计算单元执行任务的情况
 */
public class SortedTaskSlots {
    private final Logger logger = LoggerFactory.getLogger(SortedTaskSlots.class);

    private Item head; //链表头指针，不存放元素

    /**
     * 构造计算单元列表，id从1开始增加
     * @param slotNumber
     */
    public SortedTaskSlots(Integer slotNumber) {
        int id = 1;

        if (slotNumber <= 0) {
            throw new IllegalArgumentException();
        }

        head = new Item(new TaskSlot(0));

        Item item = null;
        Item prev = head;
        while (id <= slotNumber) {
            item = new Item(new TaskSlot(id++));
            item.prev = prev;
            prev.next = item;
            prev = prev.next;
        }

        prev.next = head;
        head.prev = prev;

    }

    public TaskSlot getFirst() {
        return head.next.slot;
    }

    public TaskSlot getLast() {
        return head.prev.slot;
    }

    public boolean add(TaskSlot slot) {
        //先删除失效item
        Item item = head.next;
        while (item != head) {
            if (item.slot.getId().equals(slot.getId())) {
                item.prev.next = item.next;
                item.next.prev = item.prev;
                break;
            }
            item = item.next;
        }

        //根据新的available time将item添加到链表中，保证链表依旧有序
        Item oldItem = item;
        item = head.next;
        // 从链首开始寻找插入位置
        while (item != head) {
            if (item.slot.getAvailableTime() >= slot.getAvailableTime()) {
                oldItem.next = item;
                oldItem.prev = item.prev;
                item.prev.next = oldItem;
                item.prev = oldItem;
                break;
            } else {
                item = item.next;
            }
        }

        // 插入到链尾
        if (item == head) {
            oldItem.next = head;
            oldItem.prev = head.prev;
            head.prev.next = oldItem;
            head.prev = oldItem;
        }

        return true;
    }

    public void show() {
        Item item = head.next;
        logger.info("[");
        while (item != head) {
            logger.info("<{}, {}>,", item.slot.getId(), item.slot.getAvailableTime());
            item = item.next;
        }
        logger.info("]");
    }


    public class Item {
        TaskSlot slot;
        Item next;
        Item prev;

        public Item(TaskSlot slot) {
            this.slot = slot;
        }
    }
}
