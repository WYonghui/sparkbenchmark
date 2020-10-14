package org.apache.spark.examples.aliTrace.MultistageEvaluation.common;

import org.apache.commons.cli.*;

/**
 * @Author: yonghui
 * @Date: 2020-09-29
 * @Description:
 */
public class JobCommandParser {

    private Options options;
    private String appName;

    public JobCommandParser(String appName) {
        this.appName = appName;
        this.options = new Options();

        Option option1 = new Option("s", true, "cluster, e.g., spark, rdma-spark, telescope");
        option1.setRequired(true);
        option1.setArgName("clusterType");
        options.addOption(option1);
        Option option2 = new Option("j", true, "the path of submitted job");
        option2.setRequired(true);
        option2.setArgName("jobPath");
        options.addOption(option2);
        Option option3 = new Option("p", true, "the parallelism of job");
        option3.setRequired(true);
        option3.setArgName("parallelism");
        options.addOption(option3);
        Option option4 = new Option("c", true, "the core number in the cluster");
        option4.setRequired(true);
        option4.setArgName("coreNum");
        options.addOption(option4);
        Option option5 = new Option("w", true, "weight factor");
        option5.setArgName("weightFactor");
        options.addOption(option5);
    }

    public CommandLine parse(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.getOptionValue('s').toLowerCase().equals("telescope")) {
                if (!commandLine.hasOption('w')) {
                    HelpFormatter helper = new HelpFormatter();
                    helper.printHelp(appName + " -c <coreNum> -j <jobPath> -p <parallelism> -s telescope -w <weightFactor>", options);
                    System.exit(-1);
                }
            }
        } catch (ParseException ex) {
            HelpFormatter helper = new HelpFormatter();
            helper.printHelp(appName + " -c <coreNum> -j <jobPath> -p <parallelism> -s <clusterType>", options);
            System.exit(-1);
        }
        return commandLine;
    }
}
