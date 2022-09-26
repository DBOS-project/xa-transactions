package org.dbos.apiary.xa;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
        this.postgresConnection = postgresConnection;
        this.mysqlConnection = mysqlConnection;
    }

    private void rollback(ApiaryXID xid, boolean ended) throws Exception{
        if (ended == false) {
            getXAConnection(PostgresDBType).XAEnd(xid);
            getXAConnection(MySQLDBType).XAEnd(xid);
        }
        // TODO: persist abort decision ?
        // Rollback XA transaction in underlying databases
        getXAConnection(PostgresDBType).XARollback(xid);
        getXAConnection(MySQLDBType).XARollback(xid);
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, boolean isReplay, Object... inputs) throws Exception {
        FunctionOutput f = null;
        while(true) {
            XAContext ctxt = new XAContext(this, workerContext, service, execID, functionID);
            ApiaryXID xid  = ApiaryXID.fromLong(xidCounter.getAndIncrement());
            
            boolean committed = false;
            boolean ended = false;
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
                    break;
                }
            } catch (Exception e) {
                // try again
                if (e instanceof InvocationTargetException) {
                    Throwable innerException = e;
                    while (innerException instanceof InvocationTargetException) {
                        InvocationTargetException i = (InvocationTargetException) innerException;
                        innerException = i.getCause();
                    }
                    if (innerException instanceof PSQLException) {
                        PSQLException p = (PSQLException) innerException;
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                            try {
                                rollback(xid, ended);
                                continue;
                            } catch (SQLException ex) {
                                ex.printStackTrace();
                            }
                        } else {
                            logger.info("Unrecoverable XA error: {} {}", p.getMessage(), p.getSQLState());
                        }
                    }
                }
                logger.info("Unrecoverable error in function execution: {}", e.getMessage());
                e.printStackTrace();
                break;
            }
            if (!committed) {
                rollback(xid, ended);
            }
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
        Map<Integer, String> myMap = new TreeMap<Integer, String>();
        myMap.put(0, "localhost");
        return myMap;
    }

    @Override
    public TransactionContext getLatestTransactionContext() {
        // TODO Auto-generated method stub
        return null;

    }
}
