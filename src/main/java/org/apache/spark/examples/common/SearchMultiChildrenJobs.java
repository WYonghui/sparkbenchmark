package org.apache.spark.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * java -cp spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.examples.common.SearchMultiChildrenJobs \
     E:\download\ali-cluster-Data\jobInfo.csv E:\download\ali-cluster-Data\jobInfo_selectMultiChildren.csv
 */
public class SearchMultiChildrenJobs {

    private static Logger LOG = LoggerFactory.getLogger(SearchMultiChildrenJobs.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.info("Usage: java <mainClass> <filePath> <resultPath>");
            System.exit(-1);
        }

        String path = args[0];
        String result = args[1];
        FileInputStream inputStream = new FileInputStream(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        FileOutputStream outputStream = new FileOutputStream(result);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));


        String line = null;
        while ((line = reader.readLine()) != null) {
//            分解作业信息，提取阶段及其依赖关系
            Map<String, ArrayList<String>> stageToChildren = new HashMap<>();
            String[] stages = line.split(",\\[|, |\\]");
            for (int i = 1; i < stages.length; i++) {
                String[] fathers = stages[i].split("_");
                for (int j = 1; j < fathers.length; j++) {
                    String father = fathers[j];
                    ArrayList<String> fatherToBrothers = null;
                    if (stageToChildren.containsKey(father)) {
                        fatherToBrothers = stageToChildren.get(father);
                    } else {
                        fatherToBrothers = new ArrayList<>();
                    }
                    fatherToBrothers.add(fathers[0]);
                    stageToChildren.put(father, fatherToBrothers);
                }
            }

//            遍历hash表，判断是否有某个阶段存在多个子阶段
            Iterator<ArrayList<String>> iterator = stageToChildren.values().iterator();
            while (iterator.hasNext()) {
                ArrayList<String> list= iterator.next();
                if (list.size() > 1){
                    writer.write(line);
                    writer.newLine();
                    break;
                }
            }
        }

//        String str = "j_319980,[M2, J7_1_2_3_4_5_6, M6, M5, M1, M4, M3]";
//        String[] items = str.split(",\\[|, |\\]");
//        LOG.info("Split string: " + items.length + "");
//        for (int i = 0; i < items.length; i++) {
//            LOG.info(items[i]);
//        }

        writer.flush();
        reader.close();
        inputStream.close();
        writer.close();
        outputStream.close();
    }

}
