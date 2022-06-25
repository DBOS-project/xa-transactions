package org.dbos.apiary.xa;

import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.util.internal.ThreadLocalRandom;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.procedures.GetApiaryClientID;
import org.dbos.apiary.xa.procedures.MySQLXAQueryPerson;
import org.dbos.apiary.xa.procedures.PostgresXAQueryPerson;
import org.dbos.apiary.xa.procedures.XAQueryPersonBoth;
import org.dbos.apiary.xa.procedures.XASimpleTest;
import org.dbos.apiary.xa.procedures.XAUpsertPerson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.postgresql.xa.PGXAConnection;
import org.postgresql.xa.PGXAException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class XATests {
    private static final Logger logger = LoggerFactory.getLogger(XATests.class);

    private ApiaryWorker apiaryWorker;

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testSimpleMySQL() throws InvalidProtocolBufferException {
        logger.info("testSimpleMySQL");
        try {
            MySQLXAConnection conn = new MySQLXAConnection("172.17.0.1", 3306, "dbos", "root", "dbos");
            conn.dropTable("test");
            conn.createTable("test", "a int, b varchar(100)");
            for (int i = 0; i < 10; ++i) {
                conn.executeUpdate("INSERT INTO test(a, b) VALUES (?, ?);", i, "test" + i);
            }
            ResultSet res = conn.executeQuery("SELECT * from test");
            logger.info(res.toString());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }


    @Test
    public void testSimpleMySQLXA() throws InvalidProtocolBufferException {
        logger.info("testSimpleMySQLXA");
        try {
            MySQLXAConnection conn = new MySQLXAConnection("172.17.0.1", 3306, "dbos", "root", "dbos");
            conn.dropTable("test");
            conn.createTable("test", "a int, b varchar(100)");
            ApiaryXID xid = ApiaryXID.fromLong(100);
            conn.XAStart(xid);
            for (int i = 0; i < 10; ++i) {
                conn.executeUpdate("INSERT INTO test(a, b) VALUES (?, ?);", i, "test" + i);
            }
            conn.XAEnd(xid);
            boolean vote = conn.XAPrepare(xid);
            if (vote) {
                logger.info("Transaction {} prepared", xid);
                conn.XACommit(xid);
                logger.info("Transaction {} committed", xid);
            } else {
                logger.info("Transaction {} prepare voted false", xid);
                conn.XARollback(xid);
                logger.info("Transaction {} aborted", xid);
            }
            ResultSet res = conn.executeQuery("SELECT * from test");
            logger.info(res.toString());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }


    @Test
    public void testSimplePostgres() throws InvalidProtocolBufferException {
        logger.info("testSimplePostgres");
        try {
            PostgresXAConnection conn = new PostgresXAConnection("172.17.0.1", 5432, "dbos", "postgres", "dbos");
            conn.dropTable("test");
            conn.createTable("test", "a int, b varchar(100)");
            for (int i = 0; i < 10; ++i) {
                conn.executeUpdate("INSERT INTO test(a, b) VALUES (?, ?);", i, "test" + i);
            }
            ResultSet res = conn.executeQuery("SELECT * from test");
            logger.info(res.toString());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }


    @Test
    public void testSimplePostgresXA() throws InvalidProtocolBufferException {
        logger.info("testSimplePostgresXA");
        try {
            PostgresXAConnection conn = new PostgresXAConnection("172.17.0.1", 5432, "dbos", "postgres", "dbos");
            conn.dropTable("test");
            conn.createTable("test", "a int, b varchar(100)");
            ApiaryXID xid = ApiaryXID.fromLong(100);
            conn.XAStart(xid);
            for (int i = 0; i < 10; ++i) {
                conn.executeUpdate("INSERT INTO test(a, b) VALUES (?, ?);", i, "test" + i);
            }
            conn.XAEnd(xid);
            boolean vote = conn.XAPrepare(xid);
            if (vote) {
                logger.info("Transaction {} prepared", xid);
                conn.XACommit(xid);
                logger.info("Transaction {} committed", xid);
            } else {
                logger.info("Transaction {} prepare voted false", xid);
                conn.XARollback(xid);
                logger.info("Transaction {} aborted", xid);
            }
            ResultSet res = conn.executeQuery("SELECT * from test");
            logger.info(res.toString());
        } catch (PGXAException e) {
            e.printStackTrace();
            logger.info(e.getMessage(), "error code ", e.errorCode);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSimpleXA() throws InvalidProtocolBufferException {
        logger.info("testSimpleXA");

        XAConnection conn;
        try {
            MySQLXAConnection mysqlConn = new MySQLXAConnection("172.17.0.1", 3306, "dbos", "root", "dbos");
            PostgresXAConnection postgresConn = new PostgresXAConnection("172.17.0.1", 5432, "dbos", "postgres", "dbos");    
            mysqlConn.dropTable("test");
            mysqlConn.createTable("test", "a int, b varchar(100)");
            postgresConn.dropTable("test");
            postgresConn.createTable("test", "a int, b varchar(100)");
            conn = new XAConnection(postgresConn, mysqlConn);
        } catch (Exception e) {
            logger.info("No XA instance!");
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(XAConfig.XA, conn);
        apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.XA, GetApiaryClientID::new);
        apiaryWorker.registerFunction("XASimpleTest", XAConfig.XA, XASimpleTest::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("XASimpleTest", "123").getInt();
        assertEquals(123, res);

        res = client.executeFunction("XASimpleTest", "456").getInt();
        assertEquals(456, res);
    }
    
    //@BeforeEach
    public void resetTables() {
        try {
            PostgresXAConnection postgresConn = new PostgresXAConnection("172.17.0.1", 5432, "dbos", "postgres", "dbos");    
            postgresConn.dropTable("FuncInvocations");
            postgresConn.dropTable("PersonTable");
            postgresConn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }

        try {
            MySQLXAConnection mysqlConn = new MySQLXAConnection("172.17.0.1", 3306, "dbos", "root", "dbos");
            mysqlConn.dropTable("PersonTable");
            mysqlConn.createTable("PersonTable", "Name varchar(1000) NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to MySQL.");
        }

        apiaryWorker = null;
    }


    @Test
    public void testMysqlConcurrentInsert() throws InterruptedException {
        logger.info("testMysqlConcurrentInsert");

        resetTables();
        XAConnection conn;
        try {
            MySQLXAConnection mysqlConn = new MySQLXAConnection("172.17.0.1", 3306, "dbos", "root", "dbos");
            PostgresXAConnection postgresConn = new PostgresXAConnection("172.17.0.1", 5432, "dbos", "postgres", "dbos");    
            conn = new XAConnection(postgresConn, mysqlConn);
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(XAConfig.XA, conn);
        apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.XA, GetApiaryClientID::new);
        apiaryWorker.registerFunction("XAUpsertPerson", XAConfig.XA, XAUpsertPerson::new);
        apiaryWorker.registerFunction("MySQLXAQueryPerson", XAConfig.XA, MySQLXAQueryPerson::new);
        apiaryWorker.registerFunction("PostgresXAQueryPerson", XAConfig.XA, PostgresXAQueryPerson::new);
        apiaryWorker.registerFunction("XAQueryPersonBoth", XAConfig.XA, XAQueryPersonBoth::new);
        
        apiaryWorker.startServing();


        long start = System.currentTimeMillis();
        long testDurationMs = 5000L;
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        Runnable r = () -> {
            try {
                ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
                while (System.currentTimeMillis() < start + testDurationMs) {
                    int localCount = count.getAndIncrement();
                    client.executeFunction("XAUpsertPerson", "matei" + localCount, localCount).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localCount - 5, localCount + 5);
                    int res = client.executeFunction("MySQLXAQueryPerson", search).getInt();
                    if (res == -1) {
                        success.set(false);
                    }
                    res = client.executeFunction("PostgresXAQueryPerson", search).getInt();
                    if (res == -1) {
                        success.set(false);
                    }

                    res = client.executeFunction("XAQueryPersonBoth", "matei" + localCount).getInt();
                    assert(res == 2);
                    if (res != 2) {
                        success.set(false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                success.set(false);
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        for (Thread t: threads) {
            t.join();
        }
        assertTrue(success.get());
    }

}
