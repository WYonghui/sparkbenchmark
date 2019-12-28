package org.apache.spark.examples.common;

import java.io.*;

public class ParsePokecSocialNetwork {

    public static void main(String[] args) throws Exception{

        if (args.length < 1) {
            System.exit(-1);
        }

        String path = args[0];
        FileInputStream inputStream = new FileInputStream(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        FileOutputStream outputStream = new FileOutputStream(path + "_space");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

//        处理soc-pokec-relationships
//        String line;
//        while ((line = reader.readLine()) != null) {
//            line = line.replace('\t', ' ');
//            writer.write(line);
//            writer.newLine();
//        }

//        处理soc-pokec-profiles
        String line;
        while ((line = reader.readLine()) != null) {
            String[] profiles = line.split("\t");
            String newLine = profiles[0] + "," + profiles[4];
            writer.write(newLine);
            writer.newLine();
        }

        writer.flush();
        writer.close();
        outputStream.close();
        reader.close();
        inputStream.close();

    }
}
