package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 有序的双向链表（默认升序）
 * 链表中元素包含两个属性，id和value
 * @author yonghui
 * @since 2020-09-30
 */
public class SortedList extends LinkedList<SortedList.Entry<String, Integer>> {

    @Override
    public boolean add(Entry<String, Integer> entry) {
        if (super.isEmpty()) {
            super.add(entry);
        } else {
            if (entry.value > super.getLast().value) { // 添加到末尾
                super.add(entry);
            } else {
                for (int i = 0; i < super.size(); i++) { // 添加到中间某个位置，插入后保证链表有序
                    if (super.get(i).value >= entry.value) {
                        super.add(i, entry);
                        break;

                    }
                }
            }

        }
        return true;
    }

    public boolean add(String stageId, Integer value) {
        Entry<String, Integer> entry = new Entry<>(stageId, value);
        if (super.isEmpty()) {
            super.add(entry);
        } else {
            if (entry.value > super.getLast().value) { // 添加到末尾
                super.add(entry);
            } else {
                for (int i = 0; i < super.size(); i++) { // 添加到中间某个位置，插入后保证链表有序
                    if (super.get(i).value >= entry.value) {
                        super.add(i, entry);
                        break;

                    }
                }
            }

        }
        return true;
    }

    public boolean containsId(String id) {
        Iterator<Entry<String, Integer>> iterator = super.iterator();
        while (iterator.hasNext()) {
            Entry<String, Integer> entry = iterator.next();
            if (entry.id.equals(id)) {
                return true;
            }
        }
        return false;
    }

    public static class Entry<X, Y> {
        X id;
        Y value;
//        Entry<X, Y> next;
//        Entry<X, Y> prev;

        public Entry() {
        }

        public Entry(X id, Y value) {
            this.id = id;
            this.value = value;
        }

        public X getId() {
            return id;
        }

        public Y getValue() {
            return value;
        }
    }
}
