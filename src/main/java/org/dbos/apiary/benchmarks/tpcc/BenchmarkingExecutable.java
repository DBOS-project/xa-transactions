package org.dbos.apiary.benchmarks.tpcc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingExecutable {
    private static final Logger logger = LoggerFactory.getLogger(org.dbos.apiary.benchmarks.tpcc.BenchmarkingExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "Which Benchmark?");
        options.addOption("d", true, "Benchmark Duration (sec)?");
        options.addOption("i", true, "Benchmark Interval (μs)");
        options.addOption("mainHostAddr", true, "Address of the main Apiary host to connect to.");
        options.addOption("mysqlAddr", true, "Address of the MySQL server.");
        options.addOption("postgresAddr", true, "Address of the Postgres server.");
        options.addOption("tm", true, "Which transaction manager to use? (bitronix | XDBT)");
        options.addOption("w", true, "Number of warehouses?");
        options.addOption("s", true, "Scale factor (Default: 1)");
        options.addOption("l", true, "Number of loader threads (Default: half of the available processors)");
        options.addOption("p", true, "Percentage of New-Order transactions (Default 50)");

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

        String transactionManager = "bitronix";
        if (cmd.hasOption("tm")) {
            transactionManager = cmd.getOptionValue("tm");
        }

        WorkloadConfiguration conf = new WorkloadConfiguration();
        conf.setBenchmarkName("TPCC");
        conf.setDBName("tpcc");
        conf.setDBAddressMySQL(mysqlAddr);
        conf.setDBAddressPG(postgresAddr);
        if (cmd.hasOption("s")) {
            double scaleFactor = Double.parseDouble(cmd.getOptionValue("s"));
            conf.setScaleFactor(scaleFactor);
        } else {
            conf.setScaleFactor(1);
        }
        if (cmd.hasOption("w")) {
            int warehouses = Integer.parseInt(cmd.getOptionValue("s"));
            conf.setNumWarehouses(warehouses);
        } else {
            conf.setNumWarehouses(5);
        }
        if (cmd.hasOption("l")) {
            conf.setLoaderThreads(Integer.parseInt(cmd.getOptionValue("l")));
        } else {
            if (ThreadUtil.availableProcessors() / 2 < 1) {
                conf.setLoaderThreads(1);
            } else {
                conf.setLoaderThreads(ThreadUtil.availableProcessors() / 2);
            }
        }

    
        logger.info("Running TPCC benchmark using transaction manager {}.", transactionManager);
        int percentageNewOrder = 50;
        if (cmd.hasOption("p1")) {
            percentageNewOrder = Integer.parseInt(cmd.getOptionValue("p1"));
        }
        logger.info("TPCC benchmark neworder percentage: {}%", percentageNewOrder);
        logger.info("TPCC benchmark against postgres@{}, mysql@{}", postgresAddr, mysqlAddr);
        TPCCBenchmark.benchmark(conf, transactionManager, mainHostAddr, interval, duration, percentageNewOrder);
    }
}