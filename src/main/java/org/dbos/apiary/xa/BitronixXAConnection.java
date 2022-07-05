package org.dbos.apiary.xa;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BitronixXAConnection extends XAConnection {

    private AtomicLong xidCounter = new AtomicLong(1);
    private static final Logger logger = LoggerFactory.getLogger(XAConnection.class);
    public static String PostgresDBType = "Postgres";
    public static String MySQLDBType = "MySQL";
    BitronixXADBConnection postgresConnection;
    BitronixXADBConnection mysqlConnection;
    

    public BitronixXADBConnection getXAConnection(String DBType) {
        if (DBType.equals(PostgresDBType)) {
            return postgresConnection;
        } else if (DBType.equals(MySQLDBType)) {
            return mysqlConnection;
        } else {
            return null;
        }
    }

    public BitronixXAConnection(BitronixXADBConnection postgresConnection, BitronixXADBConnection mysqlConnection) throws SQLException {
        super(postgresConnection,  mysqlConnection);
        assert(postgresConnection != null);
        assert(mysqlConnection != null);
        this.postgresConnection = postgresConnection;
        this.mysqlConnection = mysqlConnection;
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, Object... inputs) throws Exception {
        FunctionOutput f = null;
        BitronixTransactionManager btm = TransactionManagerServices.getTransactionManager();

        while(true) {
            XAContext ctxt = new XAContext(this, workerContext, service, execID, functionID);
            try {
                btm.begin();
                f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                btm.commit();
                return f;
            } catch (Exception e) {
                try {
                    btm.rollback();
                } catch (Exception ex) {
                    e.printStackTrace();
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
                            logger.info("Unrecoverable XA error: {} {}", p.getMessage(), p.getSQLState());
                        }
                    }
                }
                logger.info("Unrecoverable error in function execution: {}", e.getMessage());
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
