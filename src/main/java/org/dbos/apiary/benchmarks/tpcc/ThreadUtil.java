/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package org.dbos.apiary.benchmarks.tpcc;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

public abstract class ThreadUtil {
    private static final Logger LOG = Logger.getLogger(ThreadUtil.class);

    private static final Object lock = new Object();
    private static ExecutorService pool;

    private static final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static Integer OVERRIDE_NUM_THREADS = null;

    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * Convenience wrapper around Thread.sleep() for when we don't care about
     * exceptions
     * @param millis
     */
    public static void sleep(long millis) {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ex) {
                // IGNORE!
            }
        }
    }
    
    /**
     * Have shutdown actually means shutdown. Tasks that need to complete should use
     * futures.
     */
    public static ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor(String name, UncaughtExceptionHandler handler, int poolSize, int stackSize) {
        // HACK: ScheduledThreadPoolExecutor won't let use the handler so
        // if we're using ExceptionHandlingRunnable then we'll be able to 
        // pick up the exceptions
        Thread.setDefaultUncaughtExceptionHandler(handler);
        
        ThreadFactory factory = getThreadFactory(name, handler);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize, factory);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }
    
    public static ThreadFactory getThreadFactory(final String name, final UncaughtExceptionHandler handler) {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(null, r, name, 1024*1024);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(handler);
                return t;
            }
        };
    }

    /**
     * Get the max number of threads that will be allowed to run concurrenctly
     * in the global pool
     * 
     * @return
     */
    public static int getMaxGlobalThreads() {
        if (OVERRIDE_NUM_THREADS != null) {
            return (OVERRIDE_NUM_THREADS);
        }
        int max_threads = DEFAULT_NUM_THREADS;
        String prop = System.getProperty("hstore.max_threads");
        if (prop != null && prop.startsWith("${") == false)
            max_threads = Integer.parseInt(prop);
        return (max_threads);
    }

    public static void setMaxGlobalThreads(int max_threads) {
        OVERRIDE_NUM_THREADS = max_threads;
    }

    /**
     * Execute the given collection of Runnables in the global thread pool. The
     * calling thread will block until all of the threads finish
     * 
     * @param <R>
     * @param runnables
     */
    public static <R extends Runnable> void runGlobalPool(final Collection<R> runnables) {
        // Initialize the thread pool the first time that we run
        synchronized (ThreadUtil.lock) {
            if (ThreadUtil.pool == null) {
                int max_threads = ThreadUtil.getMaxGlobalThreads();
                if (LOG.isDebugEnabled())
                    LOG.debug("Creating new fixed thread pool [num_threads=" + max_threads + "]");
                ThreadUtil.pool = Executors.newFixedThreadPool(max_threads, factory);
            }
        } // SYNCHRONIZED
        ThreadUtil.run(runnables, ThreadUtil.pool, false);
    }
    
    public static synchronized void shutdownGlobalPool() {
        if (ThreadUtil.pool != null) {
            ThreadUtil.pool.shutdown();
            ThreadUtil.pool = null;
        }
    }

    /**
     * Execute all the given Runnables in a new pool
     * 
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runNewPool(final Collection<R> threads) {
        ExecutorService pool = Executors.newCachedThreadPool(factory);
        ThreadUtil.run(threads, pool, true);
    }

    /**
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runNewPool(final Collection<R> threads, int max_concurrent) {
        ExecutorService pool = Executors.newFixedThreadPool(max_concurrent, factory);
        ThreadUtil.run(threads, pool, true);
    }

    /**
     * For a given list of threads, execute them all (up to max_concurrent at a
     * time) and return once they have completed. If max_concurrent is null,
     * then all threads will be fired off at the same time
     * 
     * @param runnables
     * @param max_concurrent
     * @throws Exception
     */
    private static final <R extends Runnable> void run(final Collection<R> runnables, final ExecutorService pool, final boolean stop_pool) {
        final long start = System.currentTimeMillis();
        final int num_threads = runnables.size();
        final CountDownLatch latch = new CountDownLatch(num_threads);
        LatchedExceptionHandler handler = new LatchedExceptionHandler(latch);

        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Executing %d threads and blocking until they finish", num_threads));
        for (R r : runnables) {
            pool.execute(new LatchRunnable(r, latch, handler));
        } // FOR
        if (stop_pool)
            pool.shutdown();

        try {
            latch.await();
        } catch (InterruptedException ex) {
            LOG.fatal("ThreadUtil.run() was interupted!", ex);
            throw new RuntimeException(ex);
        } finally {
            if (handler.hasError()) {
                String msg = "Failed to execute threads: " + handler.getLastError().getMessage();
                throw new RuntimeException(msg, handler.getLastError());
            }
        }
        if (LOG.isDebugEnabled()) {
            final long stop = System.currentTimeMillis();
            LOG.debug(String.format("Finished executing %d threads [time=%.02fs]",
                      num_threads, (stop - start) / 1000d));
        }
        return;
    }

    private static final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return (t);
        }
    };

    private static class LatchRunnable implements Runnable {
        private final Runnable r;
        private final CountDownLatch latch;
        private final Thread.UncaughtExceptionHandler handler;

        public LatchRunnable(Runnable r, CountDownLatch latch, Thread.UncaughtExceptionHandler handler) {
            this.r = r;
            this.latch = latch;
            this.handler = handler;
        }

        @Override
        public void run() {
            Thread.currentThread().setUncaughtExceptionHandler(this.handler);
            this.r.run();
            this.latch.countDown();
        }
    }

}
