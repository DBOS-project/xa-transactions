package org.dbos.apiary.xa;

import org.dbos.apiary.benchmarks.tpcc.UserAbortException;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BitronixXAConnection extends XAConnection {

    private static final Logger logger = LoggerFactory.getLogger(XAConnection.class);
    public static String PostgresDBType = "Postgres";
    public static String MySQLDBType = "MySQL";
    BitronixXADBConnection pgConnection;
    BitronixXADBConnection mqConnection;
    BitronixTransactionManager btm = null;
    @Override
    public XADBConnection getXAConnection(String DBType) {
        if (DBType.equals(PostgresDBType)) {
            return this.pgConnection;
        } else if (DBType.equals(MySQLDBType)) {
            return this.mqConnection;
        } else {
            return null;
        }
    }
    
    public BitronixXAConnection(BitronixXADBConnection postgresConnection, BitronixXADBConnection mysqlConnection) throws SQLException {
        super(null,  null);
        btm = TransactionManagerServices.getTransactionManager();
        this.pgConnection = postgresConnection;
        this.mqConnection = mysqlConnection;
        assert(this.pgConnection != null);
        assert(this.mqConnection != null);
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, Object... inputs) throws Exception {
        FunctionOutput f = null;


        while(true) {
            XAContext ctxt = new XAContext(this, workerContext, service, execID, functionID);
            try {
                btm.begin();
                f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                btm.commit();
                return f;
            } catch (Exception e) {
                try {
                    if (btm.getCurrentTransaction() != null) {
                        btm.rollback();
                    }
                } catch (Exception ex) {
                    logger.info("rollback error");
                    ex.printStackTrace();
                }
                if (e instanceof InvocationTargetException) {
                    Throwable innerException = e;
                    while (innerException instanceof InvocationTargetException) {
                        InvocationTargetException i = (InvocationTargetException) innerException;
                        innerException = i.getCause();
                    }
                    if (innerException instanceof PSQLException) {
                        PSQLException p = (PSQLException) innerException;
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                            continue;
                        } else {
                            logger.info("Unrecoverable XA error from PG: {} {}", p.getMessage(), p.getSQLState());
                        }
                    } else if (innerException instanceof MySQLTransactionRollbackException) {
                        MySQLTransactionRollbackException m = (MySQLTransactionRollbackException) innerException;
                        if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                            continue; // Deadlock or lock timed out
                        } else {
                            logger.info("Unrecoverable XA error from MySQL: {} {} {}", m.getMessage(), m.getSQLState(), m.getErrorCode());
                        }
                    } else if (innerException instanceof UserAbortException) {
                        continue;
                    }
                } else if (e instanceof bitronix.tm.internal.BitronixRollbackException) {
                    continue;
                } else if (e instanceof UserAbortException) {
                    continue;
                }
                logger.info("Unrecoverable error in function execution: {}", e.toString());

                e.printStackTrace();
                break;
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
        // Ignore this if not necessary.
        Map<Integer, String> myMap = new HashMap<Integer, String>() {{
            put(0, "localhost");
        }};
        return myMap;
    }
}
