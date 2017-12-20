package org.apache.hadoop.yarn.client.cli;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;

public class DebugControllerCLI extends YarnCLI {

    private static final String ADD_APP     = "addapp";
    private static final String ADD_QUEUE   = "addqueue";
    private static final String REMOVE_APP  = "removeapp";
    private static final String REMOVE_QUEUE= "removequeue";
    private static final String GET_APPS    = "getapps";
    private static final String QET_QUEUES  = "getqueues";


    public static void main(String[] args) throws Exception {
        DebugControllerCLI cli = new DebugControllerCLI();
        cli.setSysOutPrintStream(System.out);
        cli.setSysErrPrintStream(System.err);
        int res = ToolRunner.run(cli, args);
        cli.stop();
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Options opts = new Options();
        opts.addOption(HELP_CMD, false, "Displays help for all commands");
        opts.addOption(ADD_APP, true,"Enable addition scheduling DEBUG on the application");
        opts.getOption(ADD_APP).setArgName("Application ID");
        opts.addOption(ADD_QUEUE, true, "Enable addition scheduling DEBUG on the queue");
        opts.getOption(ADD_QUEUE).setArgName("Queue name");

        opts.addOption(REMOVE_APP, true,"Disable addition scheduling DEBUG on the application");
        opts.getOption(REMOVE_APP).setArgName("Application ID");
        opts.addOption(REMOVE_QUEUE, true, "Disable addition scheduling DEBUG on the queue");
        opts.getOption(REMOVE_QUEUE).setArgName("Queue name");

        opts.addOption(GET_APPS, false, "Lists the applications with additional scheduling DEBUG");
        opts.addOption(QET_QUEUES, false, "Lists the queues with additional scheduling DEBUG");

        int exitCode = -1;
        CommandLine cliParser = null;
        try {
            cliParser = new GnuParser().parse(opts, args);
        } catch (MissingArgumentException ex) {
            sysout.println("Missing argument for options");
            printUsage(opts);
            return exitCode;
        }

        if (cliParser.hasOption(ADD_APP)) {
            if (args.length != 2) {
                printUsage(opts);
                return exitCode;
            }
            try{
                return addApplication(cliParser.getOptionValue(ADD_APP));
            } catch (ApplicationNotFoundException e) {
                return exitCode;
            }
        } else if (cliParser.hasOption(REMOVE_APP)) {
            if (args.length != 2) {
                printUsage(opts);
                return exitCode;
            }
            try{
                return removeApplication(cliParser.getOptionValue(REMOVE_APP));
            } catch (ApplicationNotFoundException e) {
                return exitCode;
            }
        } else if (cliParser.hasOption(GET_APPS)) {
            if (args.length != 1) {
                printUsage(opts);
                return exitCode;
            }
            return getApplications();
        } else if (cliParser.hasOption(ADD_QUEUE)) {
            if (args.length != 2) {
                printUsage(opts);
                return exitCode;
            }
            return addQueue(cliParser.getOptionValue(ADD_QUEUE));
        } else if (cliParser.hasOption(REMOVE_QUEUE)) {
            if (args.length != 2) {
                printUsage(opts);
                return exitCode;
            }
            return removeQueue(cliParser.getOptionValue(REMOVE_QUEUE));
        } else if (cliParser.hasOption(QET_QUEUES)) {
            if (args.length != 1) {
                printUsage(opts);
                return exitCode;
            }
            return listQueues();
        } else if (cliParser.hasOption(HELP_CMD)) {
            printUsage(opts);
            return 0;
        } else {
            syserr.println("Invalid Command Usage : ");
            printUsage(opts);
            return -1;
        }
    }

    /**
     * Lists the applications with additional scheduling DEBUG
     *
     * @throws YarnException
     * @throws IOException
     */
    private int getApplications() throws YarnException, IOException {
        sysout.println("Applications with additional scheduling DEBUG:");
        sysout.println(client.getDebugApps());
        return 0;
    }

    /**
     * Enable addition scheduling DEBUG on the application
     *
     * @param applicationId
     * @throws YarnException
     * @throws IOException
     */
    private int addApplication(String applicationId) throws YarnException,
            IOException {

        ApplicationReport appReport = getAppReport(applicationId);

        if (isAppFinished(appReport)) {
            sysout.println("Application " + applicationId + " has already finished ");
            return -1;
        } else {
            sysout.println("Enable addition scheduling DEBUG on the application " + applicationId);
            client.addDebugApp(applicationId);
            return 0;
        }
    }

    /**
     * Disable addition scheduling DEBUG on the application
     *
     * @param applicationId
     * @throws YarnException
     * @throws IOException
     */
    private int removeApplication(String applicationId) throws YarnException,
            IOException {

        ApplicationReport appReport = getAppReport(applicationId);

        if (isAppFinished(appReport)) {
            sysout.println("Application " + applicationId + " has already finished ");
            return -1;
        } else {
            sysout.println("Disable addition scheduling DEBUG from the application " + applicationId);
            client.removeDebugApp(applicationId);
            return 0;
        }
    }

    /**
     * Enable addition scheduling DEBUG on the queue
     *
     * @param queueName
     * @throws YarnException
     * @throws IOException
     */
    private int addQueue(String queueName) throws IOException, YarnException {

        for (QueueInfo queueInfo : client.getAllQueues()) {
            if (queueInfo.getQueueName().equals(queueName)) {
                sysout.println("Enable addition scheduling DEBUG on the queue " + queueName);
                client.addDebugQueue(queueName);
                return 0;
            }
        }
        sysout.println("There is no queue \"" + queueName + "\"!");
        return -1;
    }

    /**
     * Disable addition scheduling DEBUG on the queue
     *
     * @param queueName
     * @throws YarnException
     * @throws IOException
     */
    private int removeQueue(String queueName) throws IOException, YarnException {

        for (QueueInfo queueInfo : client.getAllQueues()) {
            if (queueInfo.getQueueName().equals(queueName)) {
                sysout.println("Disable addition scheduling DEBUG from the queue " + queueName);
                client.removeDebugQueue(queueName);
                return 0;
            }
        }
        sysout.println("There is no queue \"" + queueName + "\"!");
        return -1;
    }


    /**
     * Lists the queues with additional scheduling DEBUG
     *
     */
    private int listQueues() throws IOException, YarnException {
        sysout.println("Queues with additional scheduling DEBUG:");
        sysout.println(client.getDebugQueues());
        return 0;
    }



    private ApplicationReport getAppReport (String applicationId)
            throws YarnException, IOException {

        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        ApplicationReport appReport;
        try {
            appReport = client.getApplicationReport(appId);
        } catch (ApplicationNotFoundException e) {
            sysout.println("Application with id '" + applicationId +
                    "' doesn't exist in RM.");
            throw e;
        }
        return appReport;
    }

    private boolean isAppFinished(ApplicationReport appReport) {
        return appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
                || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
                || appReport.getYarnApplicationState() == YarnApplicationState.FAILED;
    }

    /**
     * It prints the usage of the command
     *
     * @param opts
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("debugcontrol", opts);
    }

}
