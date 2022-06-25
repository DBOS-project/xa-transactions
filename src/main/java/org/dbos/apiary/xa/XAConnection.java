package org.dbos.apiary.xa;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class XAConnection implements ApiaryConnection {

    private AtomicLong xidCounter = new AtomicLong(1);
    private static final Logger logger = LoggerFactory.getLogger(XAConnection.class);
    public static String PostgresDBType = "Postgres";
    public static String MySQLDBType = "MySQL";
    XADBConnection postgresConnection;
    XADBConnection mysqlConnection;
    
    public XADBConnection getXAConnection(String DBType) {
        if (DBType.equals(PostgresDBType)) {
            return postgresConnection;
        } else if (DBType.equals(MySQLDBType)) {
            return mysqlConnection;
        } else {
            return null;
        }
    }

    public XAConnection(XADBConnection postgresConnection, XADBConnection mysqlConnection) throws SQLException {
        assert(postgresConnection != null);
        assert(mysqlConnection != null);
        this.postgresConnection = postgresConnection;
        this.mysqlConnection = mysqlConnection;
        // TODO: implement building connections to databases.
        // Please refer to the implementation of PostgresConnection.
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, Object... inputs) throws Exception {
        // TODO: may pass more parameters to XAContext.
        XAContext ctxt = new XAContext(this, workerContext, service, execID, functionID);
        ApiaryXID xid  = ApiaryXID.fromLong(xidCounter.getAndIncrement());
        FunctionOutput f = null;
        boolean committed = false;
        boolean ended = false;
        // TODO: transaction manager does 2PC here to commit or abort the transaction.

        try {
            // Start XA transaction in underlying databases
            getXAConnection(PostgresDBType).XAStart(xid);
            getXAConnection(MySQLDBType).XAStart(xid);
            // The function would contain transactions across multiple databases.
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
            // End XA transaction in underlying databases
            getXAConnection(PostgresDBType).XAEnd(xid);
            getXAConnection(MySQLDBType).XAEnd(xid);
            ended =true;

            // Prepare-phase
            if (getXAConnection(PostgresDBType).XAPrepare(xid) && getXAConnection(MySQLDBType).XAPrepare(xid)) {
                // TODO: persist commit decision ?
                // Commit-phase
                getXAConnection(PostgresDBType).XACommit(xid);
                getXAConnection(MySQLDBType).XACommit(xid);
                committed = true;
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }

        if (committed == false) {
            if (ended == false) {
                getXAConnection(PostgresDBType).XAEnd(xid);
                getXAConnection(MySQLDBType).XAEnd(xid);
            }
            // TODO: persist abort decision ?
            // Rollback XA transaction in underlying databases
            getXAConnection(PostgresDBType).XARollback(xid);
            getXAConnection(MySQLDBType).XARollback(xid);
        }
        if (f == null) {
            f = new FunctionOutput(0, new ArrayList<org.dbos.apiary.function.Task>());
        }
        return f;
    }

    @Override
    public Set<TransactionContext> getActiveTransactions() {
        // Ignore this if not necessary.
        return null;
    }

    @Override
    public void updatePartitionInfo() {
        // Ignore this if not necessary.
    }

    @Override
    public int getNumPartitions() {
        // Ignore this if not necessary.
        return 1;
    }

    @Override
    public String getHostname(Object... objects) {
        // Ignore this if not necessary.
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        // Ignore this if not necessary.
        return Map.of(0, "localhost");
    }
}
