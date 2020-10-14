package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

import scala.tools.nsc.doc.model.Val;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 有序的双向链表（默认升序）
 * 链表中元素包含两个属性，id和value
 * @author yonghui
 * @since 2020-09-30
 */
public class SortedList<Key extends Comparable<? super Key>, Value extends Comparable<? super Value>> extends LinkedList<SortedList.Entry<Key, Value>> {

    @Override
    public boolean add(Entry<Key, Value> entry) {
        if (super.isEmpty()) {
            super.add(entry);
        } else {
            if (entry.value.compareTo(super.getLast().value) > 0) { // 添加到末尾
                super.add(entry);
            } else {
                for (int i = 0; i < super.size(); i++) { // 添加到中间某个位置，插入后保证链表有序
                    if (super.get(i).value.compareTo(entry.value) >= 0) {
                        super.add(i, entry);
                        break;

                    }
                }
            }

        }
        return true;
    }

    public boolean add(Key stageId, Value value) {
        Entry<Key, Value> entry = new Entry<>(stageId, value);
        return add(entry);
    }

    public boolean containsId(Key id) {
        Iterator<Entry<Key, Value>> iterator = super.iterator();
        while (iterator.hasNext()) {
            Entry<Key, Value> entry = iterator.next();
            if (entry.id.equals(id)) {
                return true;
            }
        }
        return false;
    }

    public boolean removeByKey(Key id) {

        boolean ret = false;
        for (int i = 0; i < super.size(); i++) {
            if (super.get(i).id.equals(id)) {
                super.remove(i);
                ret = true;
                break;
            }
        }

        return ret;
    }

    public static class Entry<X, Y> {
        X id;
        Y value;

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
