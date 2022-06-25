package org.dbos.apiary.xa;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.procedures.GetApiaryClientID;
import org.dbos.apiary.xa.procedures.XASimpleTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import org.postgresql.xa.PGXAConnection;
import org.postgresql.xa.PGXAException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XATests {
    private static final Logger logger = LoggerFactory.getLogger(XATests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {

    }

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

}
