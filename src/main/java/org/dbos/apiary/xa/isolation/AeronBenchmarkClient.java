package org.dbos.apiary.xa.isolation;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
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

import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReceiveAgent implements Agent
{
    private final Subscription subscription;
    private final ShutdownSignalBarrier barrier;
    private final int sendCount;
    private final Logger logger = LoggerFactory.getLogger(ReceiveAgent.class);
    private int bufferSize = 0;
    private byte[] readBuffer;
    private FragmentAssembler assembler = null;
    public ReceiveAgent(final Subscription subscription, ShutdownSignalBarrier barrier, int sendCount, int bufferSize)
    {
        this.bufferSize = bufferSize;
        if (bufferSize > 0) {
            readBuffer = new byte[bufferSize];
        }
        this.subscription = subscription;
        this.barrier = barrier;
        this.sendCount = sendCount;
        this.assembler = new FragmentAssembler(this::handler);
    }

    @Override
    public int doWork() throws Exception
    {
        subscription.poll(assembler, 100);
        return 0;
    }

    private void handler(DirectBuffer buffer, int offset, int length, Header header)
    {
        final int lastValue = buffer.getInt(offset);
        if (bufferSize > 0) {
            buffer.getBytes(8, readBuffer);
        }
        if (lastValue >= sendCount)
        {
            logger.info("received: {}", lastValue);
            barrier.signal();
        }
    }

    @Override
    public String roleName()
    {
        return "receiver";
    }
}

public class AeronBenchmarkClient {
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

        int bufferSize = 0;
        if (cmd.hasOption("b")) {
            bufferSize = Integer.parseInt(cmd.getOptionValue("b")) * 1024;
        }

        final String channel = "aeron:ipc";
        final int stream = 10;
        final int sendCount = iterations;
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        //construct Media Driver, cleaning up media driver folder on start/stop
        // final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
        //         .threadingMode(ThreadingMode.SHARED)
        //         .sharedIdleStrategy(new BusySpinIdleStrategy()).aeronDirectoryName("/dev/shm/aeron");
        //final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        //construct Aeron, pointing at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName("/dev/shm/aeron");
        final Aeron aeron = Aeron.connect(aeronCtx);

        logger.info("Dir {}", "/dev/shm/aeron");

        //construct the subs and pubs
        final Subscription subscription = aeron.addSubscription(channel, stream);
        //final Publication publication = aeron.addPublication(channel, stream);

        //construct the agents
        //final SendAgent sendAgent = new SendAgent(publication, sendCount);
        final ReceiveAgent receiveAgent = new ReceiveAgent(subscription, barrier, sendCount, bufferSize);

        //construct agent runners
        final AgentRunner receiveAgentRunner = new AgentRunner(idleStrategyReceive,
                Throwable::printStackTrace, null, receiveAgent);
        logger.info("starting");
        //start the runners
        //AgentRunner.startOnThread(sendAgentRunner);
        long t0 = System.nanoTime();
        AgentRunner.startOnThread(receiveAgentRunner);

        //wait for the final item to be received before closing
        barrier.await();
        long t1 = System.nanoTime();
        logger.info("Benchmark took {} seconds for {} operations, latency/op {} usec", (t1 - t0)/1000000000.0, iterations, (t1 - t0) / 1000.0 / iterations);
        //close the resources
        receiveAgentRunner.close();
        aeron.close();
        //mediaDriver.close();
    }

}
