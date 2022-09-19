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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.aeron.Publication;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

class SendAgent implements Agent
{
    private final Publication publication;
    private final int sendCount;
    private final UnsafeBuffer unsafeBuffer;
    private int currentCountItem = 1;
    private final ShutdownSignalBarrier barrier;
    private boolean signaled = false;
    private int bufferSizeKB = 0;
    private byte[] randomBytes = null;
    public SendAgent(final Publication publication, int sendCount, ShutdownSignalBarrier barrier, int bufferSizeKB)
    {
        if (bufferSizeKB > 0) {
            this.randomBytes = new byte[bufferSizeKB];
            Random random = new Random();
            random.nextBytes(randomBytes);
        }
        this.bufferSizeKB = bufferSizeKB;
        this.barrier = barrier;
        this.publication = publication;
        this.sendCount = sendCount;
        this.unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocate(8 + bufferSizeKB));
    }

    @Override
    public int doWork()
    {
        if (currentCountItem > sendCount)
        {
            if (signaled == false) {
                barrier.signal();
                signaled = true;
            }
            return 0;
        }

        if (publication.isConnected())
        {
            if (publication.offer(unsafeBuffer) > 0)
            {
                currentCountItem += 1;
                unsafeBuffer.putInt(0, currentCountItem);
                if (bufferSizeKB > 0) {
                    unsafeBuffer.putBytes(8, randomBytes);
                }
            }
        }
        return 0;
    }

    @Override
    public String roleName()
    {
        return "sender";
    }
}
public class AeronBenchmarkServer {
    private static final Logger logger = LoggerFactory.getLogger(AeronBenchmarkServer.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "buffer size (KB) ");
        options.addOption("i", true, "Benchmark iterations");
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

        final String channel = "aeron:ipc";
        final int stream = 10;
        final int sendCount = iterations;
        final IdleStrategy idleStrategySend = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        //construct Media Driver, cleaning up media driver folder on start/stop
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnShutdown(true)
                .sharedIdleStrategy(new BusySpinIdleStrategy()).aeronDirectoryName("/dev/shm/aeron");
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);
        
        //construct Aeron, pointing at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);

        logger.info("Dir {}", mediaDriver.aeronDirectoryName());

        //construct the subs and pubs
        final Publication publication = aeron.addPublication(channel, stream);

        //construct the agents
        final SendAgent sendAgent = new SendAgent(publication, sendCount, barrier, bufferSizeKB);

        //construct agent runners
        final AgentRunner sendAgentRunner = new AgentRunner(idleStrategySend,
                Throwable::printStackTrace, null, sendAgent);
        logger.info("starting");
        //start the runners
        AgentRunner.startOnThread(sendAgentRunner);

        //wait for the final item to be received before closing
        barrier.await();

        //close the resources
        sendAgentRunner.close();
        aeron.close();
        mediaDriver.close();
    }

}
