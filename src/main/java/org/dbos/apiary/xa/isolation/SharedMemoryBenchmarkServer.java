package org.dbos.apiary.xa.isolation;

import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class SharedMemoryBenchmarkServer {
    private static final Logger logger = LoggerFactory.getLogger(org.dbos.apiary.benchmarks.BenchmarkingExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "buffer size (KB) ");
        options.addOption("i", true, "Benchmark Iterations");
        options.addOption("mainHostAddr", true, "Address of the main Apiary host to connect to.");
        options.addOption("mysqlAddr", true, "Address of the MySQL server.");
        options.addOption("postgresAddr", true, "Address of the Postgres server.");
        options.addOption("p1", true, "Percentage 1");
        options.addOption("tm", true, "Which transaction manager to use? (bitronix | xinjing)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        int iterations = 1000;
        if (cmd.hasOption("i")) {
            iterations = Integer.parseInt(cmd.getOptionValue("i"));
        }

        int bufferSizeKB = 0;
        if (cmd.hasOption("b")) {
            bufferSizeKB = Integer.parseInt(cmd.getOptionValue("b")) * 1024;
        }


        // int duration = 60;
        // if (cmd.hasOption("d")) {
        //     duration = Integer.parseInt(cmd.getOptionValue("d"));
        // }

        // String mainHostAddr = "localhost";
        // if (cmd.hasOption("mainHostAddr")) {
        //     mainHostAddr = cmd.getOptionValue("mainHostAddr");
        // }

        // String postgresAddr = "localhost";
        // if (cmd.hasOption("postgresAddr")) {
        //     postgresAddr = cmd.getOptionValue("postgresAddr");
        // }

        // String mysqlAddr = "localhost";
        // if (cmd.hasOption("mysqlAddr")) {
        //     mysqlAddr = cmd.getOptionValue("mysqlAddr");
        // }

        // String transactionManager = "bitronix";
        // if (cmd.hasOption("tm")) {
        //     transactionManager = cmd.getOptionValue("tm");
        // }

        // String benchmark = cmd.getOptionValue("b");

        
        ChronicleQueue ipcClient = SingleChronicleQueueBuilder.builder(Paths.get("/dev/shm/queue-ipc-for-client"), net.openhft.chronicle.wire.WireType.BINARY_LIGHT).build();
        ChronicleQueue ipcServer = SingleChronicleQueueBuilder.builder(Paths.get("/dev/shm/queue-ipc-for-server"), net.openhft.chronicle.wire.WireType.BINARY_LIGHT).build();

        ExcerptAppender appender = ipcClient.acquireAppender();
        ExcerptTailer tailer = ipcServer.createTailer();

        AtomicInteger number = new AtomicInteger(10);
        
        
        Bytes bytes = Bytes.elasticByteBuffer();
        if (bufferSizeKB > 0) {
            byte[] arr = new byte[bufferSizeKB];
            Random rd = new Random();
            rd.nextBytes(arr);
            bytes.write(arr);
        } 
        
        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; ++i) {
            appender.writeBytes(b -> {b.writeInt(number.get());});
            if (bufferSizeKB > 0) {
                appender.writeBytes(bytes);
            }
            //logger.info("Server wrote {}", number.get());

            //logger.info("Server starts to read integer ");
            while (tailer.readBytes(b -> { number.set(b.readInt());} ) == false);

            //logger.info("Server read {}", number.get());

        }
        long t1 = System.nanoTime();
        appender.close();
        tailer.close();
        ipcClient.close();
        ipcServer.close();;

        logger.info("Benchmark took {} seconds for {} operations, latency/op {} usec", (t1 - t0)/1000000000.0, iterations, (t1 - t0) / 1000.0 / iterations);
    }

}
