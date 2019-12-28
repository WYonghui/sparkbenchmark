package org.apache.spark.examples.common;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 解析Hibench SVD生成的数据，以矩阵格式输出
 * spark-submit --master spark://inode39:7077 --deploy-mode client --class org.apache.spark.examples.common.ParseSVDGeneratorData \
     spark-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar ParseSVDGeneratorData \
     /rdma-spark-0.9.5/hibench/HiBench/SVD/Input /rdma-spark-0.9.5/hibench/HiBench/SVD/Output/
 */
public class ParseSVDGeneratorData {
    private static Logger LOG = LoggerFactory.getLogger(ParseSVDGeneratorData.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            LOG.info("Usage: ParseSVDGeneratorData <appName> <input> <output>");
        }

        String input = args[1];
        String output = args[2];

        SparkSession ss = SparkSession
                .builder()
                .appName(args[0])
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

//        读取以object形式存储的文件
        JavaRDD<Vector> vectorRDD = sc.objectFile(input);
//        转换成String
        JavaRDD<String> stringJavaRDD = vectorRDD.mapPartitions(new FlatMapFunction<Iterator<Vector>, String>() {
            @Override
            public Iterator<String> call(Iterator<Vector> vectorIterator) throws Exception {
                ArrayList<String> list = new ArrayList<>();

                while (vectorIterator.hasNext()) {
                    Vector vector = vectorIterator.next();
                    double[] ds = vector.toArray();
                    StringBuilder stringBuilder = new StringBuilder();
                    for (double d : ds) {
                        stringBuilder.append(d);
                        stringBuilder.append(" ");
                    }
                    list.add(stringBuilder.toString());
                }

                return list.iterator();
            }
        });

        stringJavaRDD.saveAsTextFile(output);
    }
}
