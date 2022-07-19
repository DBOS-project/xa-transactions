package org.dbos.apiary.xa.benchmarks;

import org.dbos.apiary.client.ApiaryWorkerClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class XABankBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(XABankBenchmark.class);
    private static final int numWorkerThreads = 3;
    private static final int threadPoolSize = 3;
    private static final int threadWarmupMs = 5000;  // First 5 seconds of requests would be warm-up and not recorded.

    private static final int numAccounts = 1000;
    private static final int initBalance = 10000; // TODO: more realistic ones?
    private static String[] DBTypes = {XAConnection.MySQLDBType, XAConnection.PostgresDBType};
    private static final int correctTotalBalance = numAccounts * initBalance * 2;

    // Use the following queues to record execution times.
    private static final Collection<Long> auditTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> transferTimes = new ConcurrentLinkedQueue<>();

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

    public static void benchmark(String transactionManager, String mainHostAddr, String postgresAddr, String mysqlAddr, Integer interval, Integer duration, int percentageTransfer) throws SQLException, InterruptedException {
        resetBankAccountTables(postgresAddr, mysqlAddr);
        XAConnection conn;
        if (transactionManager.equals("bitronix")) {
            //conn = getBitronixXAConnection(postgresAddr, mysqlAddr);
            conn = getBitronix2MySQLXAConnection(mysqlAddr);
        } else if (transactionManager.equals("xinjing")) {
            conn = getXJPGMySQLXAConnection(postgresAddr, mysqlAddr);
        } else {
            throw new RuntimeException("Unknown transaction manager " + transactionManager);
        }


        ApiaryWorker apiaryWorker = null;
        if (mainHostAddr.equalsIgnoreCase("localhost")) {
            // Start a worker in this process. Otherwise, the worker itself could be remote.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorkerThreads);
            apiaryWorker.registerConnection(XAConfig.XA, conn);
            apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.XA, GetApiaryClientID::new);
            apiaryWorker.registerFunction("BankAudit", XAConfig.XA, BankAudit::new);
            apiaryWorker.registerFunction("BankTransfer", XAConfig.XA, BankTransfer::new);

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
                if (chooser < percentageTransfer) {
                    int fromAccountId = ThreadLocalRandom.current().nextInt(numAccounts);
                    int toAccountId = ThreadLocalRandom.current().nextInt(numAccounts);
                    int fromDBTypeIdx = ThreadLocalRandom.current().nextInt(2);
                    String fromDBType = DBTypes[fromDBTypeIdx];
                    String toDBType = DBTypes[1 - fromDBTypeIdx];

                    client.get().executeFunction("BankTransfer", fromDBType, toDBType, fromAccountId, toAccountId).getInt();
                    if (warmed.get()) {
                        transferTimes.add(System.nanoTime() - t0);
                    }
                } else {
                    int sumBalance = client.get().executeFunction("BankAudit", XAConnection.MySQLDBType, XAConnection.PostgresDBType).getInt();

                    if (sumBalance != correctTotalBalance) {
                        logger.info("Inconsistency: correct balance {} != actual balance {}", correctTotalBalance, sumBalance);
                        success.set(false);
                    }
                    if (warmed.get()) {
                        auditTimes.add(System.nanoTime() - t0);
                    }
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

        List<Long> queryTimes = transferTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Transfer Updates Operations: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No transfer operations");
        }

        queryTimes = auditTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Audit Read Operations: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No audit operations");
        }

        stopped.set(true);
        threadPool.shutdown();
        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);

        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    private static void resetBankAccountTables(String postgresAddr, String mysqlAddr) throws SQLException {
        PostgresXAConnection postgresConn = new PostgresXAConnection(postgresAddr, XAConfig.postgresPort, "dbos", "postgres", "dbos");    
        postgresConn.dropTable("FuncInvocations");
        postgresConn.dropTable("BankAccount");
        postgresConn.createTable("BankAccount", "id int PRIMARY KEY NOT NULL, balance int NOT NULL");

        // Initialize the bank accounts.
        for(int i = 0; i < numAccounts; ++i) {
            postgresConn.executeUpdate("INSERT INTO BankAccount VALUES(?,?)", i, initBalance);
        }

        MySQLXAConnection mysqlConn = new MySQLXAConnection(mysqlAddr, XAConfig.mysqlPort, "dbos", "root", "dbos");
        mysqlConn.dropTable("BankAccount");
        mysqlConn.createTable("BankAccount", "id int PRIMARY KEY NOT NULL, balance int NOT NULL");
        // Initialize the bank accounts.
        for(int i = 0; i < numAccounts; ++i) {
            mysqlConn.executeUpdate("INSERT INTO BankAccount VALUES(?,?)", i, initBalance);
        }

        MySQLXAConnection mysql2Conn = new MySQLXAConnection(mysqlAddr, XAConfig.mysql2Port, "dbos", "root", "dbos");
        mysql2Conn.dropTable("BankAccount");
        mysql2Conn.createTable("BankAccount", "id int PRIMARY KEY NOT NULL, balance int NOT NULL");
        // Initialize the bank accounts.
        for(int i = 0; i < numAccounts; ++i) {
            mysql2Conn.executeUpdate("INSERT INTO BankAccount VALUES(?,?)", i, initBalance);
        }
        
        postgresConn.close();;
        mysqlConn.close();
        mysql2Conn.close();
    }
}
