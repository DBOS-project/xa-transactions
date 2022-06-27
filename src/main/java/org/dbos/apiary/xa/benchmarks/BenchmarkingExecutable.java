package org.dbos.apiary.xa.benchmarks;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingExecutable {
    private static final Logger logger = LoggerFactory.getLogger(org.dbos.apiary.benchmarks.BenchmarkingExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "Which Benchmark?");
        options.addOption("d", true, "Benchmark Duration (sec)?");
        options.addOption("i", true, "Benchmark Interval (μs)");
        options.addOption("mainHostAddr", true, "Address of the main Apiary host to connect to.");
        options.addOption("mysqlAddr", true, "Address of the MySQL server.");
        options.addOption("postgresAddr", true, "Address of the Postgres server.");
        options.addOption("p1", true, "Percentage 1");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        int interval = 1000;
        if (cmd.hasOption("i")) {
            interval = Integer.parseInt(cmd.getOptionValue("i"));
        }

        int duration = 60;
        if (cmd.hasOption("d")) {
            duration = Integer.parseInt(cmd.getOptionValue("d"));
        }

        String mainHostAddr = "localhost";
        if (cmd.hasOption("mainHostAddr")) {
            mainHostAddr = cmd.getOptionValue("mainHostAddr");
        }

        String postgresAddr = "localhost";
        if (cmd.hasOption("postgresAddr")) {
            postgresAddr = cmd.getOptionValue("postgresAddr");
        }

        String mysqlAddr = "localhost";
        if (cmd.hasOption("mysqlAddr")) {
            mysqlAddr = cmd.getOptionValue("mysqlAddr");
        }

        String benchmark = cmd.getOptionValue("b");

        if (benchmark.equalsIgnoreCase("xabank")) {
            logger.info("Running XA banking benchmark.");
            int percentageTransfer = 10;
            logger.info("XABank benchmark transfer percentage: {}%", percentageTransfer);
            XABankBenchmark.benchmark(mainHostAddr, postgresAddr, mysqlAddr, interval, duration, percentageTransfer);
        }
    }

}
