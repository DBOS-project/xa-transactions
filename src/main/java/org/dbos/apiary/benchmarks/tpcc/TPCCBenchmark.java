package org.dbos.apiary.benchmarks.tpcc;

import org.dbos.apiary.benchmarks.tpcc.procedures.XANewOrderFunction;
import org.dbos.apiary.benchmarks.tpcc.procedures.XAPaymentFunction;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTMySQLNewOrderPart;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTMySQLPaymentGetCustomerByID;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTMySQLPaymentGetCustomerByName;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTMySQLPaymentPart;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTNewOrderFunction;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTPaymentFunction;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.MysqlUpsertPerson;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.BitronixXAConnection;
import org.dbos.apiary.xa.BitronixXADBConnection;
import org.dbos.apiary.xa.MySQLXAConnection;
import org.dbos.apiary.xa.PostgresXAConnection;
import org.dbos.apiary.xa.XAConfig;
import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.procedures.BankAudit;
import org.dbos.apiary.xa.procedures.BankTransfer;
import org.dbos.apiary.xa.procedures.GetApiaryClientID;
import org.dbos.apiary.xa.procedures.xdbshim.MySQLBankTransfer;
import org.dbos.apiary.xa.procedures.xdbshim.PGBankTransfer;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TPCCBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(TPCCBenchmark.class);
    private static final int numWorkerThreads = 8;
    private static final int threadPoolSize = 8;
    private static final int threadWarmupMs = 5000;  // First 5 seconds of requests would be warm-up and not recorded.

    private static String[] DBTypes = {XAConnection.MySQLDBType, XAConnection.PostgresDBType};

    // Use the following queues to record execution times.
    private static final Collection<Long> paymentTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> newOrderTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> transactionTimes = new ConcurrentLinkedQueue<>();

    static XAConnection getBitronix2MySQLXAConnection(String mysqlAddr) throws SQLException {
        BitronixXADBConnection mysqlConn = new BitronixXADBConnection("MySQL" + UUID.randomUUID().toString(), "com.mysql.cj.jdbc.MysqlXADataSource", mysqlAddr, XAConfig.mysqlPort, "dbos", "root", "dbos");
        BitronixXADBConnection mysql2Conn = new BitronixXADBConnection("MySQL" + UUID.randomUUID().toString(), "com.mysql.cj.jdbc.MysqlXADataSource", mysqlAddr, XAConfig.mysql2Port, "dbos", "root", "dbos");
        // Hack: Use mysqlConn as postgres connection to get a setup with 2 mysql instances.
        XAConnection conn = new BitronixXAConnection(mysqlConn, mysql2Conn);
        return conn;
    }

    static XAConnection getBitronixPGMySQLXAConnection(String postgresAddr, String mysqlAddr) throws SQLException {
        BitronixXADBConnection mysqlConn = new BitronixXADBConnection("MySQL" + UUID.randomUUID().toString(), "com.mysql.cj.jdbc.MysqlXADataSource", mysqlAddr, XAConfig.mysqlPort, "dbos", "root", "dbos");
        BitronixXADBConnection postgresConn = new BitronixXADBConnection("Postgres" + UUID.randomUUID().toString(), "org.postgresql.xa.PGXADataSource", postgresAddr, XAConfig.postgresPort, "dbos", "postgres", "dbos");
        XAConnection conn = new BitronixXAConnection(postgresConn, mysqlConn);
        return conn;
    }

    static XAConnection getXJPGMySQLXAConnection(String postgresAddr, String mysqlAddr) throws SQLException {
        MySQLXAConnection mysqlConn = new MySQLXAConnection(mysqlAddr, XAConfig.mysqlPort, "dbos", "root", "dbos");
        PostgresXAConnection postgresConn = new PostgresXAConnection(postgresAddr, XAConfig.postgresPort, "dbos", "postgres", "dbos");
        XAConnection conn = new XAConnection(postgresConn, mysqlConn);
        return conn;
    }  

    static MysqlDataSource getMySQLDataSource(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) {
        MysqlDataSource ds = new MysqlDataSource();
        // Set dataSource Properties
        ds.setServerName(hostname);
        ds.setPortNumber(port);
        ds.setDatabaseName(databaseName);
        ds.setUser(databaseUsername);
        ds.setPassword(databasePassword);

        return ds;
    }

    static PGSimpleDataSource getPostgresDataSource(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {hostname});
        ds.setPortNumbers(new int[] {port});
        ds.setDatabaseName(databaseName);
        ds.setUser(databaseUsername);
        ds.setPassword(databasePassword);
        ds.setSsl(false);
        return ds;
    }

    public static void benchmark(WorkloadConfiguration conf, String transactionManager, String mainHostAddr,  Integer interval, Integer duration, int percentageNewOrder, boolean mysqlDelayLogFlush, boolean skipLoading, boolean skipBench) throws SQLException, InterruptedException {
        XAConnection conn = null;
        
        if (transactionManager.equals("bitronix")) {
            BitronixXADBConnection mysqlConn = new BitronixXADBConnection("MySQL" + UUID.randomUUID().toString(), "com.mysql.cj.jdbc.MysqlXADataSource", conf.getDBAddressMySQL(), XAConfig.mysqlPort, conf.getDBName(), "root", "dbos");
            BitronixXADBConnection postgresConn = new BitronixXADBConnection("Postgres" + UUID.randomUUID().toString(), "org.postgresql.xa.PGXADataSource", conf.getDBAddressPG(), XAConfig.postgresPort, conf.getDBName(), "postgres", "dbos");
            conn = new BitronixXAConnection(postgresConn, mysqlConn);
            if (!skipLoading) {
                TPCCLoader loader = new TPCCLoader(conf, postgresConn, mysqlConn);
                List<LoaderThread> loaders = loader.createLoaderThreads();
                ThreadUtil.runNewPool(loaders, conf.getLoaderThreads());
            }
        } else if (transactionManager.equals("XDST")) {
            if (!skipLoading) {
                TPCCLoaderXDST loader = new TPCCLoaderXDST(conf, 
                getPostgresDataSource(conf.getDBAddressPG(), XAConfig.postgresPort, conf.getDBName(), "postgres", "dbos"),
                getMySQLDataSource(conf.getDBAddressMySQL(), XAConfig.mysqlPort, conf.getDBName(), "root", "dbos"));
                List<LoaderThread> loaders = loader.createLoaderThreads();
                ThreadUtil.runNewPool(loaders, conf.getLoaderThreads());
            }
        } else {
            throw new RuntimeException("Unknown transaction manager " + transactionManager);
        }

        if (skipLoading) {
            logger.info("TPCC data loading skipped");
        } else {
            logger.info("TPCC data loading finished");
        }
        
        if (skipBench) {
            logger.info("TPCC benchmark skipped");
            return;
        }

        MysqlConnection mconn;
        PostgresConnection pconn;
        try {
            mconn = new MysqlConnection(conf.getDBAddressMySQL(), XAConfig.mysqlPort, conf.getDBName(), "root", "dbos");
            pconn = new PostgresConnection(conf.getDBAddressPG(), XAConfig.postgresPort, conf.getDBName(), "postgres", "dbos");
            if (mysqlDelayLogFlush) {
                mconn.enableDelayedFlush();
            }
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

        ApiaryWorker apiaryWorker = null;
        if (mainHostAddr.equalsIgnoreCase("localhost")) {
            // Start a worker in this process. Otherwise, the worker itself could be remote.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorkerThreads);
            if (transactionManager.equals("bitronix")) {
                apiaryWorker.registerConnection(XAConfig.XA, conn);
                apiaryWorker.registerFunction("XANewOrderFunction", XAConfig.XA, XANewOrderFunction::new);
                apiaryWorker.registerFunction("XAPaymentFunction", XAConfig.XA, XAPaymentFunction::new);
                apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.XA, GetApiaryClientID::new);
            } else {
                apiaryWorker.registerConnection(XAConfig.postgres, pconn);
                apiaryWorker.registerConnection(XAConfig.mysql, mconn);
                apiaryWorker.registerFunction("XDSTPaymentFunction", XAConfig.postgres, XDSTPaymentFunction::new);
                apiaryWorker.registerFunction("XDSTNewOrderFunction", XAConfig.postgres, XDSTNewOrderFunction::new);
                apiaryWorker.registerFunction("XDSTMySQLNewOrderPart", XAConfig.mysql, XDSTMySQLNewOrderPart::new);
                apiaryWorker.registerFunction("XDSTMySQLPaymentPart", XAConfig.mysql, XDSTMySQLPaymentPart::new);
                apiaryWorker.registerFunction("XDSTMySQLPaymentGetCustomerByID", XAConfig.mysql, XDSTMySQLPaymentGetCustomerByID::new);
                apiaryWorker.registerFunction("XDSTMySQLPaymentGetCustomerByName", XAConfig.mysql, XDSTMySQLPaymentGetCustomerByName::new);
                //apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.postgres, GetApiaryClientID::new);
            }
            apiaryWorker.startServing();
        }

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient(mainHostAddr));

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);
        AtomicBoolean warmed = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(true);

        Runnable r = () -> {
            if (stopped.get() == true) {
                return;
            }
            try {
                long t0 = System.nanoTime();
                int chooser = ThreadLocalRandom.current().nextInt(100);
                int warehouseId;
                do {
                    warehouseId = ThreadLocalRandom.current().nextInt(conf.getNumWarehouses()) + 1;
                } while (TPCCLoader.getDBType(warehouseId).equals(TPCCConstants.DBTYPE_POSTGRES) == false);

                if (chooser < percentageNewOrder) {
                    if (transactionManager.equals("bitronix")) {
                        client.get().executeFunction("XANewOrderFunction", warehouseId, conf.getNumWarehouses()).getInt();
                    } else {
                        client.get().executeFunction("XDSTNewOrderFunction", warehouseId, conf.getNumWarehouses()).getInt();
                    }
                    if (warmed.get()) {
                        newOrderTimes.add(System.nanoTime() - t0);
                    }
                } else {
                    if (transactionManager.equals("bitronix")) {
                        client.get().executeFunction("XAPaymentFunction", warehouseId, conf.getNumWarehouses()).getInt();
                    } else {
                        client.get().executeFunction("XDSTPaymentFunction", warehouseId, conf.getNumWarehouses()).getInt();
                    }
                    if (warmed.get()) {
                        paymentTimes.add(System.nanoTime() - t0);
                    }
                }
                if (warmed.get()) {
                    transactionTimes.add(System.nanoTime() - t0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        long currentTime = System.currentTimeMillis();
        while (currentTime < endTime) {
            long t = System.nanoTime();
            if (!warmed.get() && ((currentTime - startTime) > threadWarmupMs)) {
                // Finished warmup, start recording.
                warmed.set(true);
                logger.info("Warmed up");
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
            currentTime = System.currentTimeMillis();
        }
        warmed.set(false);
        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;

        if (success.get()) {
            logger.info("All succeeded!");
        } else {
            logger.info("Inconsistency happened.");
        }
        
        List<Long> queryTimes = newOrderTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("New order transactions: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No new order transactions");
        }

        queryTimes = paymentTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Payment transactions: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No payment transactions");
        }

        queryTimes = transactionTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = transactionTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Total Operations: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        }
        stopped.set(true);
        threadPool.shutdown();
        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
        logger.info("MySQL upsert stats: count {}, avg latency {}, p50 latency {}, p75 latency {}, p90 latency {}, p95 latency {}, p99 latency {} ", mconn.upserts.size(), mconn.upserts.average(), mconn.upserts.nth(50), mconn.upserts.nth(75), mconn.upserts.nth(90), mconn.upserts.nth(95), mconn.upserts.nth(99));

        logger.info("MySQL query stats: count {}, avg latency {}, p50 latency {}, p75 latency {}, p90 latency {}, p95 latency {}, p99 latency {} ", mconn.queries.size(), mconn.queries.average(), mconn.queries.nth(50), mconn.queries.nth(75), mconn.queries.nth(90),mconn.queries.nth(95), mconn.queries.nth(99));

        logger.info("MySQL commit stats: count {}, avg latency {}, p50 latency {}, p75 latency {}, p90 latency {}, p95 latency {}, p99 latency {} ", mconn.commits.size(), mconn.commits.average(), mconn.commits.nth(50), mconn.commits.nth(75), mconn.commits.nth(90), mconn.commits.nth(95), mconn.commits.nth(99));

        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }
}
