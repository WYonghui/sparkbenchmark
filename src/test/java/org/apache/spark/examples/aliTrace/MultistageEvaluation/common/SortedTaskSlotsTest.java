package org.apache.spark.examples.aliTrace.MultistageEvaluation.common; 

import org.junit.Test;

/** 
* SortedTaskSlots Tester. 
* 
* @author yonghui
* @since <pre>09/29/2020</pre> 
* @version 1.0 
*/ 
public class SortedTaskSlotsTest { 

    @Test
    public static void main(String[] args) {
        // Test goes here...
        SortedTaskSlots taskSlots = new SortedTaskSlots(3);
        TaskSlot slot = taskSlots.getFirst();
        slot.setAvailableTime(slot.getAvailableTime() + 6);
        taskSlots.add(slot);

        taskSlots.show();

        slot = taskSlots.getFirst();
        slot.setAvailableTime(slot.getAvailableTime() + 4);
        taskSlots.add(slot);

        taskSlots.show();

        slot = taskSlots.getFirst();
        slot.setAvailableTime(slot.getAvailableTime() + 8);
        taskSlots.add(slot);

        taskSlots.show();

        slot = taskSlots.getFirst();
        slot.setAvailableTime(slot.getAvailableTime() + 2);
        taskSlots.add(slot);

        taskSlots.show();
    }
    

} 
